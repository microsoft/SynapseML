import os
import shutil
import json
import unittest
from pyspark.sql import SparkSession, Row
from pyspark.ml import PipelineModel, Transformer
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.types import ArrayType, IntegerType
import pyspark.sql.functions as f
from synapse.ml.stages import SelectColumns
import synapse.ml.lightgbm as lgbm

class StringArrayToVectorTransformer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    """
    A pure-Python Transformer that converts a string representation of an integer array
    into a Dense Vector. For example, it converts "[1, 2, 3]" into a DenseVector.
    """
    def __init__(self, inputCol="embedded_object_keys_string", outputCol="features", maximumVectorLength=3):
        super(StringArrayToVectorTransformer, self).__init__()
        self.inputCol = inputCol
        self.outputCol = outputCol
        self.maximumVectorLength = maximumVectorLength

    def _transform(self, dataset):
        def parse_string_embedding(s):
            try:
                # Convert a string like "[1, 2, 3]" to a list of integers.
                return json.loads(s)
            except Exception:
                return []
        parse_udf = f.udf(parse_string_embedding, ArrayType(IntegerType()))
        
        def to_vector(arr):
            # Pad or truncate the list to ensure it has exactly `maximumVectorLength` elements.
            if len(arr) < self.maximumVectorLength:
                arr = arr + [0] * (self.maximumVectorLength - len(arr))
            return Vectors.dense(arr[:self.maximumVectorLength])
        vector_udf = f.udf(to_vector, VectorUDT())
        
        df = dataset.withColumn(self.outputCol, parse_udf(f.col(self.inputCol)))
        return df.withColumn(self.outputCol, vector_udf(f.col(self.outputCol)))

class LightGBMSerializationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("LightGBMSerializationTests") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        # Define a temporary directory for saving models/pipelines.
        self.temp_dir = "./tmp/lightgbm_serialization_test"
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def tearDown(self):
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_lightgbm_model_serialization(self):
        """
        Tests that a standalone LightGBM model can be serialized and reloaded without errors.
        """
        spark = self.spark
        data = [
            Row(embedded_object_keys_string="[1, 2, 3]", label=1),
            Row(embedded_object_keys_string="[4, 5, 6]", label=0),
            Row(embedded_object_keys_string="[7, 8, 9]", label=1)
        ]
        df = spark.createDataFrame(data)
        transformer = StringArrayToVectorTransformer(
            inputCol="embedded_object_keys_string", outputCol="features", maximumVectorLength=3)
        transformed_df = transformer.transform(df)

        # Train a LightGBM model on the transformed data.
        model = lgbm.LightGBMClassifier(featuresCol="features", labelCol="label").fit(transformed_df)
        model_path = os.path.join(self.temp_dir, "lgbm_model")
        model.write().overwrite().save(model_path)
        reloaded_model = lgbm.LightGBMClassificationModel.load(model_path)
        result = reloaded_model.transform(transformed_df)
        # Verify that the prediction column is present.
        self.assertIn("prediction", result.columns)
        self.assertIn("features", result.columns)
        self.assertIn("label", result.columns)

    def test_pipeline_serialization(self):
        """
        Tests that a pipeline combining a pure-Python transformer,
        a LightGBM model, and a Java-backed transformer can be serialized and reloaded.
        """
        spark = self.spark
        data = [
            Row(embedded_object_keys_string="[1, 2, 3]", objectKey="obj1", PreciseTimeStamp="2025-03-12 00:00:00", label=1),
            Row(embedded_object_keys_string="[4, 5, 6]", objectKey="obj2", PreciseTimeStamp="2025-03-12 01:00:00", label=0),
            Row(embedded_object_keys_string="[7, 8, 9]", objectKey="obj3", PreciseTimeStamp="2025-03-12 02:00:00", label=1)
        ]
        df = spark.createDataFrame(data)
        transformer = StringArrayToVectorTransformer(
            inputCol="embedded_object_keys_string", outputCol="features", maximumVectorLength=3)
        transformed_df = transformer.transform(df)
        model = lgbm.LightGBMClassifier(featuresCol="features", labelCol="label").fit(transformed_df)
        # A Java-backed transformer to select desired output columns.
        selector = SelectColumns(cols=["objectKey", "PreciseTimeStamp", "prediction"])

        # Build the pipeline with the pure-Python transformer, LightGBM model, and Java class.
        pipeline = PipelineModel(stages=[transformer, model, selector])
        pipeline.write().overwrite().save(self.temp_dir)
        reloaded_pipeline = PipelineModel.load(self.temp_dir)
        result = reloaded_pipeline.transform(df)
        # Verify that the reloaded pipeline produces a DataFrame with the expected columns.
        self.assertIn("prediction", result.columns)
        self.assertIn("objectKey", result.columns)
        self.assertIn("PreciseTimeStamp", result.columns)


if __name__ == "__main__":
    unittest.main()
