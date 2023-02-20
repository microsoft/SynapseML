# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from pyspark import SparkContext

def template(expr):
    ctx = SparkContext._active_spark_context
    return ctx._jvm.com.microsoft.azure.synapse.ml.core.spark.Functions.template(expr)
