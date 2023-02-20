# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

# Prepare training and test data.
import unittest

from pyspark.sql import types as t, functions as f
import synapse.ml.core.spark.functions as SF
from synapsemltest.spark import *


class TemplateSpec(unittest.TestCase):
    def create_sample_dataframe(self):
        schema = t.StructType(
            [
                t.StructField("x", t.StringType(), nullable=True),
                t.StructField("y", t.IntegerType(), nullable=True),
            ],
        )

        return sc.createDataFrame(
            [
                ("1", "Bob"),
                ("2", "Mandy"),
            ],
            schema,
        )

    def test_template(self):
        df = self.create_sample_dataframe()

        output = df.select(SF.template("{x}_{yz}"))

        self.assertEqual(["1_Bob","2_Mandy"], result)



if __name__ == "__main__":
    result = unittest.main()
