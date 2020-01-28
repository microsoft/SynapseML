# Prepare training and test data.
import unittest

from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext, SQLContext
import os
from mmlspark.nn.ConditionalBallTree import ConditionalBallTree

import pickle

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("TestConditionalBallTree") \
    .config("spark.jars.packages", "com.microsoft.ml.spark:mmlspark_2.11:" + os.environ["MML_VERSION"]) \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()


class NNSpec(unittest.TestCase):

    def test_bindings(self):
        cbt = ConditionalBallTree([[1.0, 2.0], [2.0, 3.0]], [1, 2], ['foo', 'bar'], 50)

        def test_cbt(cbt_model):
            result = cbt_model.findMaximumInnerProducts([1.0, 2.0], {'foo'}, 5)
            expected = [(0, 5.0)]
            self.assertEqual(expected, result)

        test_cbt(cbt)

        # TODO make serialization test
        # cbt.save("cbt.model")
        # cbt2 = ConditionalBallTree.load("cbt.model")
        # test_cbt(cbt2)


if __name__ == "__main__":
    result = unittest.main()
