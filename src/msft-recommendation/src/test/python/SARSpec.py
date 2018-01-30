# Prepare training and test data.
import pyspark
import unittest
from mmlspark.SAR import SAR

from pyspark.ml.tuning import *
from pyspark.sql.types import *


class SARSpec(unittest.TestCase):
    def test_simple(self):
        spark = pyspark.sql.SparkSession.builder.master("local[*]") \
            .config('spark.driver.extraClassPath',
                    "/home/dciborow/mmlspark2/BuildArtifacts/packages/m2/com/microsoft/ml/spark/mmlspark_2.11/0.0/mmlspark_2.11-0.0.jar") \
            .getOrCreate()

        cSchema = StructType([StructField("customerID", IntegerType()),
                              StructField("itemID", IntegerType()),
                              StructField("rating", IntegerType()),
                              StructField("notTime", IntegerType())])

        ratings = spark.createDataFrame([
            (0, 1, 4, 4),
            (0, 3, 1, 1),
            (0, 4, 5, 5),
            (0, 5, 3, 3),
            (0, 7, 3, 3),
            (0, 9, 3, 3),
            (0, 10, 3, 3),
            (1, 1, 4, 4),
            (1, 2, 5, 5),
            (1, 3, 1, 1),
            (1, 6, 4, 4),
            (1, 7, 5, 5),
            (1, 8, 1, 1),
            (1, 10, 3, 3),
            (2, 1, 4, 4),
            (2, 2, 1, 1),
            (2, 3, 1, 1),
            (2, 4, 5, 5),
            (2, 5, 3, 3),
            (2, 6, 4, 4),
            (2, 8, 1, 1),
            (2, 9, 5, 5),
            (2, 10, 3, 3),
            (3, 2, 5, 5),
            (3, 3, 1, 1),
            (3, 4, 5, 5),
            (3, 5, 3, 3),
            (3, 6, 4, 4),
            (3, 7, 5, 5),
            (3, 8, 1, 1),
            (3, 9, 5, 5),
            (3, 10, 3, 3)], cSchema)

        # customerIndex = StringIndexer() \
        #     .setInputCol("customerIDOrg") \
        #     .setOutputCol("customerID")
        #
        # ratingsIndex = StringIndexer() \
        #     .setInputCol("itemIDOrg") \
        #     .setOutputCol("itemID")
        #
        # pipeline = Pipeline(stages=[customerIndex, ratingsIndex])
        #
        # transformedDf = pipeline.fit(ratings).transform(ratings)

        sar = SAR() \
            .setUserCol("customerID") \
            .setRatingCol('rating') \
            .setItemCol("itemID") \
            .setSupportThreshold(4)

        sarModel = sar.fit(ratings)
        # usersRecs = sarModel._call_java("recommendForAllUsers", 3)
        usersRecs = sarModel.recommendForAllUsers(3)
        print(usersRecs.take(4))
        #
        # paramGrid = ParamGridBuilder() \
        #     .addGrid(alsWReg.regParam, [1.0]) \
        #     .build()
        #
        # evaluator = MsftRecommendationEvaluator().setSaveAll(True)
        #
        # tvRecommendationSplit = TrainValidRecommendSplit() \
        #     .setEstimator(sar) \
        #     .setEvaluator(evaluator) \
        #     .setEstimatorParamMaps(paramGrid) \
        #     .setTrainRatio(0.8) \
        #     .setUserCol(customerIndex.getOutputCol()) \
        #     .setRatingCol('rating') \
        #     .setItemCol(ratingsIndex.getOutputCol())
        #
        # tvmodel = tvRecommendationSplit.fit(transformedDf)
        #
        # usersRecs = tvmodel.bestModel._call_java("recommendForAllUsers", 3)


if __name__ == '__main__':
    unittest.main()
