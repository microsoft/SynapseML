import pyspark

from pyspark.ml.tuning import *
from pyspark.sql.types import *


class RecommendTestHelper:
    def getSpark(self):
        spark = pyspark.sql.SparkSession.builder.master("local[*]") \
            .config('spark.driver.extraClassPath',
                    "/home/dciborow/mmlspark2/BuildArtifacts/packages/m2/com/microsoft/ml/spark/mmlspark_2.11/0.0/mmlspark_2.11-0.0.jar") \
            .getOrCreate()

        return spark
