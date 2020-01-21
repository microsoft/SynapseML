# Prepare training and test data.
import unittest

from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext, SQLContext
import os
from mmlspark.nn.ConditionalBallTree import ConditionalBallTree

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("TestConditionalBallTree") \
    .config("spark.jars.packages", "com.microsoft.ml.spark:mmlspark_2.11:" + os.environ["MML_VERSION"]) \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

class NNSpec(unittest.TestCase):

    def test_bindings(self):
        cbt = ConditionalBallTree([[1.0, 2.0], [2.0, 3.0]], [1, 2], ['foo', 'bar'], 50)
        result = cbt.findMaximumInnerProducts([1.0, 2.0], {'foo'}, 5)
        expected = [(0, 5.0)]
        self.assertEqual(expected, result)


if __name__ == "__main__":
    result = unittest.main()
