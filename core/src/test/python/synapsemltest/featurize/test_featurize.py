# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import math
import unittest

from pyspark.ml import Pipeline
from pyspark.sql import types as t

from synapse.ml.core.init_spark import init_spark
from synapse.ml.featurize import Featurize

spark = init_spark()


class FeaturizeSpec(unittest.TestCase):
    def test_keep_preserves_missing_numeric_rows_in_pipeline(self):
        schema = t.StructType(
            [
                t.StructField("id", t.IntegerType(), nullable=False),
                t.StructField("value", t.DoubleType(), nullable=True),
            ],
        )
        dataset = spark.createDataFrame(
            [(0, 1.0), (1, None), (2, float("nan")), (3, 4.0)],
            schema,
        )
        featurize = (
            Featurize()
            .setInputCols(["value"])
            .setOutputCol("features")
            .setImputeMissing(False)
            .setVectorAssemblerHandleInvalid("keep")
        )

        self.assertEqual(featurize.getVectorAssemblerHandleInvalid(), "keep")
        result = Pipeline(stages=[featurize]).fit(dataset).transform(dataset)
        by_id = {
            row.id: row.features[0] for row in result.select("id", "features").collect()
        }

        self.assertEqual(set(by_id), {0, 1, 2, 3})
        self.assertEqual(by_id[0], 1.0)
        self.assertTrue(math.isnan(by_id[1]))
        self.assertTrue(math.isnan(by_id[2]))
        self.assertEqual(by_id[3], 4.0)


if __name__ == "__main__":
    unittest.main()
