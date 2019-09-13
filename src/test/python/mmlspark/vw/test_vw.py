# Prepare training and test data.
import os
import pyspark
import unittest
from mmlspark.vw.VowpalWabbitClassifier import VowpalWabbitClassifier

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml.tuning import *
from pyspark.sql.types import *
from pyspark.sql import SQLContext, SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("_VW") \
    .config("spark.jars.packages", "com.microsoft.ml.spark:mmlspark_2.11:" + os.environ["MML_VERSION"]) \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

sc = spark.sparkContext

class VowpalWabbitClassificationSpec(unittest.TestCase):

    def save_model_test(self):
        vw = VowpalWabbitClassifier()
        # vw.fit()

if __name__ == "__main__":
    result = unittest.main()
