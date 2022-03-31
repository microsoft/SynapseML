from pyspark.sql import SparkSession
import os
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("SynapseML Docker App") \
    .config("spark.jars.packages", "com.microsoft.azure:synapseml_2.12:" + os.environ["MMLSPARK_VERSION"] + ",org.apache.hadoop:hadoop-azure:3.3.2,com.microsoft.azure:azure-storage:8.6.6") \
    .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven,https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-azure,https://mvnrepository.com/artifact/com.microsoft.azure/azure-storage") \
    .getOrCreate()
sc = spark.sparkContext
