# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys

if sys.version >= '3':
    basestring = str

from mmlspark.io.http._JSONOutputParser import _JSONOutputParser
from pyspark.ml.common import inherit_doc
from pyspark import SparkContext
import json
from pyspark.sql.types import StructType


@inherit_doc
class JSONOutputParser(_JSONOutputParser):

    def setDataType(self, value):
        jdt = SparkContext.getOrCreate()._jvm.org.apache.spark.sql.types.DataType.fromJson(value.json())
        self._java_obj = self._java_obj.setDataType(jdt)
        return self

    def getDataType(self):
        jdt = self._java_obj.getDataType()
        dt = StructType.fromJson(json.loads(jdt.json()))
        return dt
