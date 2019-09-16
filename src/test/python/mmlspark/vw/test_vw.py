# Prepare training and test data.
import os
import unittest
import tempfile
import pyspark

from mmlspark.vw.VowpalWabbitClassifier import VowpalWabbitClassifier
from mmlspark.vw.VowpalWabbitFeaturizer import VowpalWabbitFeaturizer

from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("_VW") \
    .config("spark.jars.packages", "com.microsoft.ml.spark:mmlspark_2.11:" + os.environ["MML_VERSION"]) \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

sc = spark.sparkContext

class VowpalWabbitClassificationSpec(unittest.TestCase):

    def test_save_model_classification(self):
        # create sample data
        schema = StructType([StructField("label", DoubleType()),
                              StructField("text", StringType())])

        data = pyspark.sql.SparkSession.builder.getOrCreate().createDataFrame([
            (-1.0, "mountains are nice"),
            ( 1.0, "do you have the TPS reports ready?")], schema)

        # featurize data
        featurizer = VowpalWabbitFeaturizer(stringSplitInputCols=['text'])
        featurized_data = featurizer.transform(data)

        # train model
        vw = VowpalWabbitClassifier()
        model = vw.fit(featurized_data)

        # write model to file and validate it's there
        with tempfile.TemporaryDirectory() as tmpdirname:
            modelFile = '{}/model'.format(tmpdirname)

            model.saveNativeModel(modelFile)

            self.assertTrue(os.stat(modelFile) > 0)

    def test_save_model_regression(self):
        # create sample data
        schema = StructType([StructField("label", DoubleType()),
                              StructField("text", StringType())])

        data = pyspark.sql.SparkSession.builder.getOrCreate().createDataFrame([
            (-1.0, "mountains are nice"),
            ( 1.0, "do you have the TPS reports ready?")], schema)

        # featurize data
        featurizer = VowpalWabbitFeaturizer(stringSplitInputCols=['text'])
        featurized_data = featurizer.transform(data)

        # train model
        vw = VowpalWabbitRegressor()
        model = vw.fit(featurized_data)

        # write model to file and validate it's there
        with tempfile.TemporaryDirectory() as tmpdirname:
            modelFile = '{}/model'.format(tmpdirname)

            model.saveNativeModel(modelFile)

            self.assertTrue(os.stat(modelFile) > 0)

if __name__ == "__main__":
    result = unittest.main()
