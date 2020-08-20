# Prepare training and test data.
import os
import unittest
import tempfile
import pyspark

from mmlsparktest.spark import *

from mmlspark.vw import VowpalWabbitContextualBandit
from mmlspark.vw import VowpalWabbitFeaturizer
from mmlspark.vw import VectorZipper

from pyspark.ml.tuning import ParamGridBuilder
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.wrapper import *
from pyspark.ml.common import inherit_doc, _java2py, _py2java
from pyspark.sql.utils import AnalysisException

def has_column(df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False

class VowpalWabbitSpec(unittest.TestCase):
    def get_data(self):
        # create sample data
        schema = StructType([
            StructField("shared_text", StringType()),
            StructField("action1", StringType()),
            StructField("action2_feat1", StringType()),
            StructField("action2_feat2", StringType()),
            StructField("chosenAction", IntegerType()),
            StructField("label", DoubleType()),
            StructField("probability", DoubleType())
        ])

        data = pyspark.sql.SparkSession.builder.getOrCreate().createDataFrame([
            ("shared_f", "action1_f", "action2_f", "action2_f2=0", 1, 1.0, 0.8),
            ("shared_f", "action1_f", "action2_f", "action2_f2=1", 2, 1.0, 0.8),
            ("shared_f", "action1_f", "action2_f", "action2_f2=1", 2, 4.0, 0.8),
            ("shared_f", "action1_f", "action2_f", "action2_f2=0", 1, 0.0, 0.8)], schema)\
            .coalesce(1)

        # featurize data
        shared_featurizer = VowpalWabbitFeaturizer(inputCols=['shared_text'],
                                                   outputCol='shared')
        action_one_featurizer = VowpalWabbitFeaturizer(
            inputCols=['action1'], outputCol='action1_features')
        action_two_featurizer = VowpalWabbitFeaturizer(
            inputCols=['action2_feat1', 'action2_feat2'],
            outputCol='action2_features')
        action_merger = VectorZipper(
            inputCols=['action1_features', 'action2_features'],
            outputCol='features')
        pipeline = Pipeline(stages=[
            shared_featurizer, action_one_featurizer, action_two_featurizer,
            action_merger
        ])
        tranformation_pipeline = pipeline.fit(data)

        return tranformation_pipeline.transform(data)

    def get_data_two_shared(self):
        # create sample data
        schema = StructType([
            StructField("shared_text", StringType()),
            StructField("shared_text2", StringType()),
            StructField("action1", StringType()),
            StructField("action2_feat1", StringType()),
            StructField("action2_feat2", StringType()),
            StructField("chosenAction", IntegerType()),
            StructField("label", DoubleType()),
            StructField("probability", DoubleType())
        ])

        data = pyspark.sql.SparkSession.builder.getOrCreate().createDataFrame([
            ("shared_f", "shared_f2", "action1_f", "action2_f", "action2_f2=0", 1, 1.0, 0.8),
            ("shared_f", "shared_f2", "action1_f", "action2_f", "action2_f2=1", 2, 1.0, 0.8),
            ("shared_f", "shared_f2", "action1_f", "action2_f", "action2_f2=1", 2, 4.0, 0.8),
            ("shared_f", "shared_f2", "action1_f", "action2_f", "action2_f2=0", 1, 0.0, 0.8)], schema) \
            .coalesce(1)

        # featurize data
        shared_featurizer = VowpalWabbitFeaturizer(inputCols=['shared_text'],
                                                   outputCol='shared')
        shared_featurizer2 = VowpalWabbitFeaturizer(inputCols=['shared_text2'],
                                                    outputCol='shared2')
        action_one_featurizer = VowpalWabbitFeaturizer(
            inputCols=['action1'], outputCol='action1_features')
        action_two_featurizer = VowpalWabbitFeaturizer(
            inputCols=['action2_feat1', 'action2_feat2'],
            outputCol='action2_features')
        action_merger = VectorZipper(
            inputCols=['action1_features', 'action2_features'],
            outputCol='features')
        pipeline = Pipeline(stages=[
            shared_featurizer, shared_featurizer2, action_one_featurizer,
            action_two_featurizer, action_merger
        ])
        tranformation_pipeline = pipeline.fit(data)
        return tranformation_pipeline.transform(data)

    def save_model(self, estimator):
        model = estimator.fit(self.get_data())

        # write model to file and validate it's there
        with tempfile.TemporaryDirectory() as tmpdirname:
            modelFile = '{}/model'.format(tmpdirname)

            model.saveNativeModel(modelFile)

            self.assertTrue(os.stat(modelFile).st_size > 0)

    def test_save_model(self):
        self.save_model(VowpalWabbitContextualBandit().setArgs(
            "--cb_explore_adf --epsilon 0.2 --quiet").
                        setUseBarrierExecutionMode(False))

    def test_initial_model(self):
        featurized_data = self.get_data()
        estimator1 = VowpalWabbitContextualBandit()\
            .setArgs("--cb_explore_adf --epsilon 0.2 --quiet")\
            .setUseBarrierExecutionMode(False)

        model1 = estimator1.fit(featurized_data)

        estimator2 = VowpalWabbitContextualBandit()\
            .setArgs("--cb_explore_adf --epsilon 0.2 --quiet")\
            .setUseBarrierExecutionMode(False)
        estimator2.setInitialModel(model1)

        estimator2.fit(featurized_data)

    def test_performance_statistics(self):
        featurized_data = self.get_data()
        estimator1 = VowpalWabbitContextualBandit()\
            .setArgs("--cb_explore_adf --epsilon 0.2 --quiet")\
            .setUseBarrierExecutionMode(False)
        model1 = estimator1.fit(featurized_data)

        stats = model1.getPerformanceStatistics()
        assert float(stats.first()["ipsEstimate"]) > 0
        assert float(stats.first()["snipsEstimate"]) > 0

    def test_parallel_fit(self):
        featurized_data = self.get_data()
        estimator1 = VowpalWabbitContextualBandit() \
            .setArgs("--cb_explore_adf --epsilon 0.2 --quiet") \
            .setParallelism(6) \
            .setUseBarrierExecutionMode(False)

        paramGrid = ParamGridBuilder() \
            .addGrid(estimator1.learningRate, [0.5, 0.1, 0.005, 0.14]) \
            .addGrid(estimator1.l2, [10.0, 20.0]) \
            .build()

        models = estimator1.parallelFit(featurized_data, paramGrid)
        models[0].getPerformanceStatistics().show()

    def test_setAdditionalSharedFeatures(self):
        featurized_data = self.get_data_two_shared()
        estimator1 = VowpalWabbitContextualBandit() \
            .setArgs("--cb_explore_adf --epsilon 0.2 --quiet") \
            .setAdditionalSharedFeatures(["shared2"]) \
            .setUseBarrierExecutionMode(False)
        model1 = estimator1.fit(featurized_data)

        stats = model1.getPerformanceStatistics()
        assert float(stats.first()["ipsEstimate"]) > 0
        assert float(stats.first()["snipsEstimate"]) > 0

    def test_model_prediction(self):
        featurized_data = self.get_data_two_shared()
        estimator1 = VowpalWabbitContextualBandit() \
            .setArgs("--cb_explore_adf --epsilon 0.2 --quiet") \
            .setAdditionalSharedFeatures(["shared2"]) \
            .setPredictionCol("output_prediction")\
            .setUseBarrierExecutionMode(False)
        model1 = estimator1.fit(featurized_data)
        df = model1.transform(featurized_data)
        assert has_column(df, "output_prediction")

if __name__ == "__main__":
    result = unittest.main()
