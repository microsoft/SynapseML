# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import unittest

from pyspark.ml.linalg import VectorUDT, Vectors
from pyspark.sql import types as t

from synapse.ml.core.init_spark import init_spark
from synapse.ml.featurize import CountSelectorModel, Featurize

spark = init_spark()


class EmptyFeaturizedTextSpec(unittest.TestCase):
    def test_empty_count_selector_model_preserves_schema_and_nulls(self):
        schema = t.StructType(
            [
                t.StructField("id", t.IntegerType(), nullable=False),
                t.StructField("features", VectorUDT(), nullable=True),
            ],
        )
        dataset = spark.createDataFrame(
            [(0, Vectors.sparse(4, [], [])), (1, None)],
            schema,
        )
        model = CountSelectorModel(
            indices=[],
            inputCol="features",
            outputCol="selected",
        )

        transformed = model.transform(dataset)
        by_id = {
            row.id: row.selected
            for row in transformed.select("id", "selected").collect()
        }

        self.assertEqual(by_id[0].size, 0)
        self.assertIsNone(by_id[1])
        self.assertTrue(transformed.schema["selected"].nullable)
        self.assertEqual(
            transformed.schema["selected"].metadata["ml_attr"]["num_attrs"],
            0,
        )

    def test_featurize_ignores_collapsed_text_when_numeric_features_remain(self):
        dataset = spark.createDataFrame(
            [(0.0, "2", 1.0), (1.0, "2", 2.0), (2.0, "2", 3.0)],
            ["label", "text", "numeric"],
        )
        model = Featurize(
            inputCols=["text", "numeric"],
            outputCol="features",
            numFeatures=1024,
        ).fit(dataset)

        features = [
            row.features.toArray().tolist()
            for row in model.transform(dataset).select("features").collect()
        ]

        self.assertEqual(features, [[1.0], [2.0], [3.0]])

    def test_featurize_rejects_an_all_collapsed_text_feature_set(self):
        dataset = spark.createDataFrame([("2",), ("2",), ("2",)], ["text"])

        with self.assertRaisesRegex(Exception, "No usable featurized features"):
            Featurize(
                inputCols=["text"],
                outputCol="features",
                numFeatures=1024,
            ).fit(dataset)


if __name__ == "__main__":
    unittest.main()
