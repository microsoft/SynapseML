# Prepare training and test data.
import os
import unittest
import tempfile
import pyspark

from synapse.ml.vw.VowpalWabbitClassifier import VowpalWabbitClassifier
from synapse.ml.vw.VowpalWabbitRegressor import VowpalWabbitRegressor
from synapse.ml.vw.VowpalWabbitFeaturizer import VowpalWabbitFeaturizer

from pyspark.sql.types import *
from synapsemltest.spark import *


class VowpalWabbitSpec(unittest.TestCase):

    def get_data(self):
        # create sample data
        schema = StructType([StructField("label", DoubleType()),
                              StructField("text", StringType())])

        data = pyspark.sql.SparkSession.builder.getOrCreate().createDataFrame([
            (-1.0, "mountains are nice"),
            ( 1.0, "do you have the TPS reports ready?")], schema)

        # featurize data
        featurizer = VowpalWabbitFeaturizer(stringSplitInputCols=['text'])
        return featurizer.transform(data)

    def save_model(self, estimator):
        model = estimator.fit(self.get_data())

        # write model to file and validate it's there
        with tempfile.TemporaryDirectory() as tmpdirname:
            modelFile = '{}/model'.format(tmpdirname)

            model.saveNativeModel(modelFile)

            self.assertTrue(os.stat(modelFile).st_size > 0)

    def test_save_model_classification(self):
        self.save_model(VowpalWabbitClassifier())

    def test_save_model_regression(self):
        self.save_model(VowpalWabbitRegressor())

    def test_initial_model(self):
        featurized_data = self.get_data()
        estimator1 = VowpalWabbitClassifier()

        model1 = estimator1.fit(featurized_data)

        estimator2 = VowpalWabbitClassifier()
        estimator2.setInitialModel(model1)

        model2 = estimator2.fit(featurized_data)

if __name__ == "__main__":
    result = unittest.main()
