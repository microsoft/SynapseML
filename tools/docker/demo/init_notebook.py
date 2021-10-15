from pyspark.sql import SparkSession
import os
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("SynapseML Docker App") \
    .config("spark.jars.packages", "com.microsoft.azure:synapseml:" + os.environ["MMLSPARK_VERSION"]) \
    .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven") \
    .getOrCreate()
sc = spark.sparkContext
