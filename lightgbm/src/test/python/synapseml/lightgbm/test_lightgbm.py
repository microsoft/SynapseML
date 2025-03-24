import unittest
from pyspark.ml import PipelineModel, Transformer
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml.linalg import Vectors
import pyspark.sql.functions as f
import synapse.ml.lightgbm as lgbm
from synapse.ml.core.init_spark import *
from pyspark.sql import SQLContext, Row
import tempfile

spark = init_spark()
sc = SQLContext(spark.sparkContext)

class PurePythonCustomTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    """
    A pure-Python Transformer for testing serializability. Returns a string.
    """
    def __init__(self, outputCol="other_column"):
        super(PurePythonCustomTransformer, self).__init__()
        self.outputCol = outputCol

    def _transform(self, dataset):
        return dataset.withColumn(self.outputCol, f.lit("hello_world"))

class LightGBMSerializationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        feature_data = Vectors.dense([0, 1, 2])
        data = [
            Row(features=feature_data, label=1),
            Row(features=feature_data, label=0),
            Row(features=feature_data, label=1)
        ]
        cls.df = spark.createDataFrame(data)
        cls.model = lgbm.LightGBMClassifier(featuresCol="features", labelCol="label").fit(cls.df)

    def test_lightgbm_model_serialization(self):
        """
        Tests that a standalone LightGBM fitted model can be serialized and reloaded without errors.
        """
        with tempfile.TemporaryDirectory() as tmpdirname:
            self.model.write().overwrite().save(tmpdirname)
            reloaded_model = lgbm.LightGBMClassificationModel.load(tmpdirname)
            result = reloaded_model.transform(self.df)

            # Verify that the prediction column is present.
            self.assertIn("prediction", result.columns)
            self.assertIn("label", result.columns)

    def test_pipeline_serialization(self):
        """
        Tests that a pipeline combining a pure-Python transformer,
        and a LightGBM fitted model can be serialized and reloaded.
        """
        transformer = PurePythonCustomTransformer()

        # Build the pipeline with the pure-Python transformer, LightGBM model, and Java class.
        pipeline = PipelineModel(stages=[transformer, self.model])

        with tempfile.TemporaryDirectory() as tmpdirname:
            pipeline.write().overwrite().save(tmpdirname)
            reloaded_pipeline = PipelineModel.load(tmpdirname)
            result = reloaded_pipeline.transform(self.df)

            # Verify that the reloaded pipeline produces a DataFrame with the expected columns.
            self.assertIn("prediction", result.columns)
            self.assertIn("label", result.columns)


if __name__ == "__main__":
    unittest.main()
