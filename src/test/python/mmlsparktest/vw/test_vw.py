# Prepare training and test data.
import os
import unittest
import tempfile
import pyspark

from mmlspark.vw.VowpalWabbitClassifier import VowpalWabbitClassifier
from mmlspark.vw.VowpalWabbitRegressor import VowpalWabbitRegressor
from mmlspark.vw.VowpalWabbitFeaturizer import VowpalWabbitFeaturizer

from pyspark.sql.types import *
from mmlsparktest.spark import *


class VowpalWabbitSpec(unittest.TestCase):

    def save_model(self, estimator):
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
        model = estimator.fit(featurized_data)

        # write model to file and validate it's there
        with tempfile.TemporaryDirectory() as tmpdirname:
            modelFile = '{}/model'.format(tmpdirname)

            model.saveNativeModel(modelFile)

            self.assertTrue(os.stat(modelFile).st_size > 0)

    def test_save_model_classification(self):
        self.save_model(VowpalWabbitClassifier())

    def test_save_model_regression(self):
        self.save_model(VowpalWabbitRegressor())

if __name__ == "__main__":
    result = unittest.main()
