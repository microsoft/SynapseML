# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

# Prepare training and test data.
import unittest

from pyspark.sql import types as t, functions as f
import synapse.ml.core.spark.functions as SF
from pyspark.sql import SQLContext
from synapse.ml.core.init_spark import *

spark = init_spark()
sc = SQLContext(spark.sparkContext)


class TemplateSpec(unittest.TestCase):
    def create_sample_dataframe(self):
        schema = t.StructType(
            [
                t.StructField("x", t.IntegerType(), nullable=True),
                t.StructField("yz", t.StringType(), nullable=True),
            ],
        )

        return sc.createDataFrame(
            [
                (1, "Bob"),
                (2, "Mandy"),
            ],
            schema,
        )

    def test_template(self):
        df = self.create_sample_dataframe()

        output = df.select(SF.template("{x}_{yz}")).toPandas().iloc[:, 0].values

        # a and b have the same elements in the same number, regardless of their order
        self.assertCountEqual(["1_Bob", "2_Mandy"], output)


if __name__ == "__main__":
    result = unittest.main()
