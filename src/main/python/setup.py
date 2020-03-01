# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import os
from setuptools import setup, find_packages
import pyspark

setup(
    name="dciborow-mmlspark-dev",
    version="0.0.1",
    description="Microsoft ML for Spark",
    long_description="Microsoft ML for Apache Spark contains Microsoft's open source " +
                     "contributions to the Apache Spark ecosystem",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        'pyspark'
    ]
    # Project's main homepage.
    url="https://github.com/Azure/mmlspark",
    # Author details
    author="Microsoft",
    author_email="mmlspark-support@microsoft.com",

    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Data Scientists",
        "Topic :: Software Development :: Datascience Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3"
    ],

    zip_safe=True,

    package_data={"mmlspark": ["../LICENSE.txt", "../README.txt"]}
)

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Install MMLSpark") \
    .config("spark.jars.packages", "com.microsoft.ml.spark:mmlspark_2.11:0.18.1") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()
