from pyspark.sql import SparkSession
import os
import pyspark
# spark = SparkSession.builder \
#     .master("local[*]") \
#     .appName("SynapseML Docker App") \
#     .config("spark.jars.packages", "com.microsoft.azure:synapseml_2.12:" + os.environ["MMLSPARK_VERSION"] + ",org.apache.hadoop:hadoop-azure:3.3.2,com.microsoft.azure:azure-storage:8.6.6") \
#     .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven,https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-azure,https://mvnrepository.com/artifact/com.microsoft.azure/azure-storage") \
#     .getOrCreate()
#     ("spark.hadoop.fs.wasbs.impl", "org.apache.hadoop.fs.azure.Wasbs"),
# stop the sparkContext and set new conf
# spark.sparkContext.stop()

syanpseMLConf = pyspark.SparkConf().setAll([
    ("spark.hadoop.fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb"),
    ("spark.hadoop.fs.AbstractFileSystem.wasbs.impl", "org.apache.hadoop.fs.azure.Wasbs"),
    ("spark.hadoop.fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem"),
    ("spark.hadoop.fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem$Secure"),
    ("spark.hadoop.fs.viewfs.overload.scheme.target.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem"),
    ("spark.jars.packages", "com.microsoft.azure:synapseml_2.12:" + os.environ["MMLSPARK_VERSION"] + ",org.apache.hadoop:hadoop-azure:2.7.0,org.apache.hadoop:hadoop-common:2.7.0,com.microsoft.azure:azure-storage:2.0.0"),
    ("spark.jars.repositories", "https://mmlspark.azureedge.net/maven,https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-azure,https://mvnrepository.com/artifact/com.microsoft.azure/azure-storage")
])

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("SynapseML Docker App") \
    .config(conf=syanpseMLConf) \
    .getOrCreate()
sc = spark.SparkContext
