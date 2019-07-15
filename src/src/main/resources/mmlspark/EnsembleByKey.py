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
class EnsembleByKey(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """
    The ``EnsembleByKey`` first performs a grouping operation on a set of keys,
    and then averages the selected columns. It can handle scalar or vector columns,
    and the dimensions of the vector columns are automatically inferred by materializing
    the first row of the column. To avoid materialization you can provide the vector dimensions
    through the ``setVectorDims`` function, which takes a mapping from
    columns (String) to dimension (Int). You can also choose to squash or keep the original
    dataset with the ``collapseGroup`` parameter.

    Args:

        colNames (list): Names of the result of each col
        collapseGroup (bool): Whether to collapse all items in group to one entry (default: true)
        cols (list): Cols to ensemble
        keys (list): Keys to group by
        strategy (str): How to ensemble the scores, ex: mean (default: mean)
        vectorDims (dict): the dimensions of any vector columns, used to avoid materialization
    """

    @keyword_only
    def __init__(self, colNames=None, collapseGroup=True, cols=None, keys=None, strategy="mean", vectorDims=None):
        super(EnsembleByKey, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.EnsembleByKey")
        self.colNames = Param(self, "colNames", "colNames: Names of the result of each col")
        self.collapseGroup = Param(self, "collapseGroup", "collapseGroup: Whether to collapse all items in group to one entry (default: true)")
        self._setDefault(collapseGroup=True)
        self.cols = Param(self, "cols", "cols: Cols to ensemble")
        self.keys = Param(self, "keys", "keys: Keys to group by")
        self.strategy = Param(self, "strategy", "strategy: How to ensemble the scores, ex: mean (default: mean)")
        self._setDefault(strategy="mean")
        self.vectorDims = Param(self, "vectorDims", "vectorDims: the dimensions of any vector columns, used to avoid materialization")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, colNames=None, collapseGroup=True, cols=None, keys=None, strategy="mean", vectorDims=None):
        """
        Set the (keyword only) parameters

        Args:

            colNames (list): Names of the result of each col
            collapseGroup (bool): Whether to collapse all items in group to one entry (default: true)
            cols (list): Cols to ensemble
            keys (list): Keys to group by
            strategy (str): How to ensemble the scores, ex: mean (default: mean)
            vectorDims (dict): the dimensions of any vector columns, used to avoid materialization
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setColNames(self, value):
        """

        Args:

            colNames (list): Names of the result of each col

        """
        self._set(colNames=value)
        return self


    def getColNames(self):
        """

        Returns:

            list: Names of the result of each col
        """
        return self.getOrDefault(self.colNames)


    def setCollapseGroup(self, value):
        """

        Args:

            collapseGroup (bool): Whether to collapse all items in group to one entry (default: true)

        """
        self._set(collapseGroup=value)
        return self


    def getCollapseGroup(self):
        """

        Returns:

            bool: Whether to collapse all items in group to one entry (default: true)
        """
        return self.getOrDefault(self.collapseGroup)


    def setCols(self, value):
        """

        Args:

            cols (list): Cols to ensemble

        """
        self._set(cols=value)
        return self


    def getCols(self):
        """

        Returns:

            list: Cols to ensemble
        """
        return self.getOrDefault(self.cols)


    def setKeys(self, value):
        """

        Args:

            keys (list): Keys to group by

        """
        self._set(keys=value)
        return self


    def getKeys(self):
        """

        Returns:

            list: Keys to group by
        """
        return self.getOrDefault(self.keys)


    def setStrategy(self, value):
        """

        Args:

            strategy (str): How to ensemble the scores, ex: mean (default: mean)

        """
        self._set(strategy=value)
        return self


    def getStrategy(self):
        """

        Returns:

            str: How to ensemble the scores, ex: mean (default: mean)
        """
        return self.getOrDefault(self.strategy)


    def setVectorDims(self, value):
        """

        Args:

            vectorDims (dict): the dimensions of any vector columns, used to avoid materialization

        """
        self._set(vectorDims=value)
        return self


    def getVectorDims(self):
        """

        Returns:

            dict: the dimensions of any vector columns, used to avoid materialization
        """
        return self.getOrDefault(self.vectorDims)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.EnsembleByKey"

    @staticmethod
    def _from_java(java_stage):
        module_name=EnsembleByKey.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".EnsembleByKey"
        return from_java(java_stage, module_name)
