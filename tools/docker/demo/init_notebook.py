from pyspark.sql import SparkSession
import os
import pyspark

syanpseMLConf = pyspark.SparkConf().setAll(
    [
        (
            "spark.hadoop.fs.AbstractFileSystem.wasb.impl",
            "org.apache.hadoop.fs.azure.Wasb",
        ),
        (
            "spark.hadoop.fs.AbstractFileSystem.wasbs.impl",
            "org.apache.hadoop.fs.azure.Wasbs",
        ),
        (
            "spark.hadoop.fs.wasb.impl",
            "org.apache.hadoop.fs.azure.NativeAzureFileSystem",
        ),
        (
            "spark.hadoop.fs.wasbs.impl",
            "org.apache.hadoop.fs.azure.NativeAzureFileSystem",
        ),
        (
            "spark.hadoop.fs.viewfs.overload.scheme.target.wasb.impl",
            "org.apache.hadoop.fs.azure.NativeAzureFileSystem",
        ),
        (
            "spark.jars.packages",
            "com.microsoft.azure:synapseml_2.13:"
            + os.getenv("SYNAPSEML_VERSION", "1.1.0")
            + ",org.apache.hadoop:hadoop-azure:2.7.0,org.apache.hadoop:hadoop-common:2.7.0,com.microsoft.azure:azure-storage:2.0.0",
        ),
        (
            "spark.jars.repositories",
            "https://mmlspark.azureedge.net/maven,https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-azure,https://mvnrepository.com/artifact/com.microsoft.azure/azure-storage",
        ),
    ],
)

spark = (
    SparkSession.builder.master("local[*]")
    .appName("SynapseML Docker App")
    .config(conf=syanpseMLConf)
    .getOrCreate()
)
sc = spark.sparkContext
