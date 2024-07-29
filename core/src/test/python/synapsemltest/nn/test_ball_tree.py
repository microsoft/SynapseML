# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

# Prepare training and test data.
import unittest

from synapse.ml.nn.ConditionalBallTree import ConditionalBallTree
from pyspark.sql import SQLContext
from synapse.ml.core.init_spark import *

spark = init_spark()
sc = SQLContext(spark.sparkContext)


class NNSpec(unittest.TestCase):
    def test_bindings(self):
        cbt = ConditionalBallTree([[1.0, 2.0], [2.0, 3.0]], [1, 2], ["foo", "bar"], 50)

        def test_cbt(cbt_model):
            result = cbt_model.findMaximumInnerProducts([1.0, 2.0], {"foo"}, 5)
            expected = [(0, 5.0)]
            self.assertEqual(expected, result)

        test_cbt(cbt)

        # TODO make serialization test
        # cbt.save("cbt.model")
        # cbt2 = ConditionalBallTree.load("cbt.model")
        # test_cbt(cbt2)


if __name__ == "__main__":
    result = unittest.main()
