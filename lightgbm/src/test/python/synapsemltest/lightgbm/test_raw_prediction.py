# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import os
import tempfile
import unittest
from pyspark.ml import PipelineModel
from pyspark.ml.classification import OneVsRest, OneVsRestModel
from pyspark.ml.feature import SQLTransformer
from pyspark.ml.param.shared import HasRawPredictionCol
from pyspark.ml.linalg import VectorUDT, Vectors
import synapse.ml.lightgbm as lgbm
from synapse.ml.core.init_spark import init_spark
from pyspark.sql import Row

spark = init_spark()


class LightGBMRawPredictionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data = [
            Row(features=Vectors.dense([value]), label=float(value > 0))
            for value in [-3.0, -2.0, -1.0, 1.0, 2.0, 3.0]
        ]
        cls.df = spark.createDataFrame(data).coalesce(1)
        cls.classifier = lgbm.LightGBMClassifier(
            numIterations=5,
            numThreads=1,
            numTasks=1,
            minDataInLeaf=1,
            numLeaves=3,
            verbosity=-1,
        )
        # Fitting caches binning metadata, so keep the shared estimator unfitted.
        cls.model = cls.classifier.copy().fit(cls.df)

    def test_lightgbm_model_serialization(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            self.model.write().overwrite().save(tmpdirname)
            reloaded_model = lgbm.LightGBMClassificationModel.load(tmpdirname)
            result = reloaded_model.transform(self.df)

            self.assertIn("prediction", result.columns)
            self.assertIn("label", result.columns)
            self.assertIsInstance(reloaded_model, HasRawPredictionCol)
            self.assertEqual(reloaded_model.getRawPredictionCol(), "rawPrediction")
            self.assertEqual(
                result.select("rawPrediction", "prediction").collect(),
                self.model.transform(self.df)
                .select("rawPrediction", "prediction")
                .collect(),
            )

    def test_raw_prediction_contract(self):
        classifier = lgbm.LightGBMClassifier()

        for stage in [classifier, self.model]:
            with self.subTest(stage=type(stage).__name__):
                self.assertIsInstance(stage, HasRawPredictionCol)
                self.assertEqual(stage.getRawPredictionCol(), "rawPrediction")
                self.assertEqual(stage.rawPredictionCol.parent, stage.uid)
                self.assertEqual(
                    [p.name for p in stage.params].count("rawPredictionCol"), 1
                )

        for stage_type in [
            lgbm.LightGBMRegressor,
            lgbm.LightGBMRegressionModel,
            lgbm.LightGBMRanker,
            lgbm.LightGBMRankerModel,
        ]:
            with self.subTest(stage=stage_type.__name__):
                self.assertFalse(issubclass(stage_type, HasRawPredictionCol))

    def test_raw_prediction_column_copy_and_persistence(self):
        classifier = self.classifier.copy(
            {self.classifier.rawPredictionCol: "confidence"}
        )
        self.assertIsInstance(classifier, HasRawPredictionCol)
        self.assertEqual(self.classifier.getRawPredictionCol(), "rawPrediction")
        self.assertEqual(classifier.getRawPredictionCol(), "confidence")

        with tempfile.TemporaryDirectory() as directory:
            classifier_path = os.path.join(directory, "classifier")
            classifier.save(classifier_path)
            reloaded_classifier = lgbm.LightGBMClassifier.load(classifier_path)
            self.assertIsInstance(reloaded_classifier, HasRawPredictionCol)
            self.assertEqual(reloaded_classifier.getRawPredictionCol(), "confidence")

            model = reloaded_classifier.fit(self.df)
            self.assertIsInstance(model, HasRawPredictionCol)
            self.assertEqual(model.getRawPredictionCol(), "confidence")
            result = model.transform(self.df)
            self.assertIsInstance(result.schema["confidence"].dataType, VectorUDT)
            self.assertNotIn("rawPrediction", result.columns)

            model_path = os.path.join(directory, "model")
            model.save(model_path)
            reloaded_model = lgbm.LightGBMClassificationModel.load(model_path)
            self.assertIsInstance(reloaded_model, HasRawPredictionCol)
            self.assertEqual(reloaded_model.getRawPredictionCol(), "confidence")
            self.assertEqual(
                result.select("confidence", "prediction").collect(),
                reloaded_model.transform(self.df)
                .select("confidence", "prediction")
                .collect(),
            )

            copied_model = reloaded_model.copy(
                {reloaded_model.rawPredictionCol: "copiedConfidence"}
            )
            self.assertIsInstance(copied_model, HasRawPredictionCol)
            self.assertEqual(reloaded_model.getRawPredictionCol(), "confidence")
            self.assertEqual(copied_model.getRawPredictionCol(), "copiedConfidence")
            copied_result = copied_model.transform(self.df)
            self.assertIsInstance(
                copied_result.schema["copiedConfidence"].dataType, VectorUDT
            )
            self.assertNotIn("confidence", copied_result.columns)

            self.assertIs(copied_model.setRawPredictionCol(""), copied_model)
            self.assertNotIn(
                "copiedConfidence", copied_model.transform(self.df).columns
            )

    def test_one_vs_rest_fit_transform_and_persistence(self):
        data = spark.createDataFrame(
            [
                Row(features=Vectors.dense([label * 10.0 + offset]), label=float(label))
                for label in range(3)
                for offset in range(6)
            ]
        ).coalesce(1)
        model = OneVsRest(classifier=self.classifier, parallelism=1).fit(data)
        self.assertEqual(len(model.models), 3)
        for binary_model in model.models:
            self.assertIsInstance(binary_model, HasRawPredictionCol)

        predictions = model.transform(data).select("rawPrediction", "prediction")
        self.assertIsInstance(predictions.schema["rawPrediction"].dataType, VectorUDT)
        expected = predictions.collect()
        self.assertEqual(len(expected), 18)
        self.assertEqual({row.prediction for row in expected}, {0.0, 1.0, 2.0})
        for row in expected:
            self.assertEqual(len(row.rawPrediction), 3)
            self.assertEqual(row.prediction, float(row.rawPrediction.argmax()))

        copied = model.copy()
        self.assertEqual(
            copied.transform(data).select("rawPrediction", "prediction").collect(),
            expected,
        )
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "one_vs_rest")
            model.save(path)
            reloaded = OneVsRestModel.load(path)
            for binary_model in reloaded.models:
                self.assertIsInstance(binary_model, HasRawPredictionCol)
            self.assertEqual(
                reloaded.transform(data)
                .select("rawPrediction", "prediction")
                .collect(),
                expected,
            )

    def test_pipeline_serialization(self):
        transformer = SQLTransformer(
            statement="SELECT *, 'hello_world' AS other_column FROM __THIS__"
        )
        pipeline = PipelineModel(stages=[transformer, self.model])

        with tempfile.TemporaryDirectory() as tmpdirname:
            pipeline.write().overwrite().save(tmpdirname)
            reloaded_pipeline = PipelineModel.load(tmpdirname)
            result = reloaded_pipeline.transform(self.df)

            self.assertIsInstance(reloaded_pipeline.stages[-1], HasRawPredictionCol)
            self.assertEqual(
                reloaded_pipeline.stages[-1].getRawPredictionCol(), "rawPrediction"
            )
            self.assertIsInstance(result.schema["rawPrediction"].dataType, VectorUDT)
            self.assertIn("prediction", result.columns)
            self.assertIn("label", result.columns)
            self.assertEqual(
                {row.other_column for row in result.select("other_column").collect()},
                {"hello_world"},
            )


if __name__ == "__main__":
    unittest.main()
