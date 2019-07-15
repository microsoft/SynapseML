# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.


import sys
if sys.version >= '3':
    basestring = str

from pyspark.ml.param.shared import *
from pyspark import keyword_only
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaTransformer, JavaEstimator, JavaModel
from pyspark.ml.common import inherit_doc
from mmlspark.Utils import *

@inherit_doc
class SelectColumns(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """
    ``SelectColumns`` takes a list of column names and returns a DataFrame
    consisting of only those columns.  Any columns in the DataFrame that are
    not in the selection list are dropped.

    :Example:

    >>> import pandas as pd
    >>> from mmlspark import SelectColumns
    >>> from pyspark.sql import SQLContext
    >>> spark = pyspark.sql.SparkSession.builder.appName("Test SelectCol").getOrCreate()
    >>> tmp1 = {"col1": [1, 2, 3, 4, 5],
    ...         "col2": [6, 7, 8, 9, 10],
    ...         "col2": [5, 4, 3, 2, 1] }
    >>> pddf = pd.DataFrame(tmp1)
    >>> pddf.columns
    ['col1', 'col2', 'col3']
    >>> data2 = SelectColumns(cols = ["col1", "col2"]).transform(data)
    >>> data2.columns
    ['col1', 'col2']

    Args:

        cols (list): Comma separated list of selected column names
    """

    @keyword_only
    def __init__(self, cols=None):
        super(SelectColumns, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.SelectColumns")
        self.cols = Param(self, "cols", "cols: Comma separated list of selected column names")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, cols=None):
        """
        Set the (keyword only) parameters

        Args:

            cols (list): Comma separated list of selected column names
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setCols(self, value):
        """

        Args:

            cols (list): Comma separated list of selected column names

        """
        self._set(cols=value)
        return self


    def getCols(self):
        """

        Returns:

            list: Comma separated list of selected column names
        """
        return self.getOrDefault(self.cols)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.SelectColumns"

    @staticmethod
    def _from_java(java_stage):
        module_name=SelectColumns.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".SelectColumns"
        return from_java(java_stage, module_name)
