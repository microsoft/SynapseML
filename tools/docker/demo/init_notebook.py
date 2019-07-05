from pyspark.sql import SparkSession
import os
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("MMLSpark Docker App") \
    .config("spark.jars.packages", "com.microsoft.ml.spark:mmlspark_2.11:" + os.environ["MMLSPARK_VERSION"]) \
    .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven") \
    .getOrCreate()
sc = spark.sparkContext
