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
class PageSplitter(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        boundaryRegex (str): how to split into words (default: \s)
        inputCol (str): The name of the input column
        maximumPageLength (int): the maximum number of characters to be in a page (default: 5000)
        minimumPageLength (int): the the minimum number of characters to have on a page in order to preserve work boundaries (default: 4500)
        outputCol (str): The name of the output column (default: [self.uid]_output)
    """

    @keyword_only
    def __init__(self, boundaryRegex="\s", inputCol=None, maximumPageLength=5000, minimumPageLength=4500, outputCol=None):
        super(PageSplitter, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.PageSplitter")
        self.boundaryRegex = Param(self, "boundaryRegex", "boundaryRegex: how to split into words (default: \s)")
        self._setDefault(boundaryRegex="\s")
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column")
        self.maximumPageLength = Param(self, "maximumPageLength", "maximumPageLength: the maximum number of characters to be in a page (default: 5000)")
        self._setDefault(maximumPageLength=5000)
        self.minimumPageLength = Param(self, "minimumPageLength", "minimumPageLength: the the minimum number of characters to have on a page in order to preserve work boundaries (default: 4500)")
        self._setDefault(minimumPageLength=4500)
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column (default: [self.uid]_output)")
        self._setDefault(outputCol=self.uid + "_output")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, boundaryRegex="\s", inputCol=None, maximumPageLength=5000, minimumPageLength=4500, outputCol=None):
        """
        Set the (keyword only) parameters

        Args:

            boundaryRegex (str): how to split into words (default: \s)
            inputCol (str): The name of the input column
            maximumPageLength (int): the maximum number of characters to be in a page (default: 5000)
            minimumPageLength (int): the the minimum number of characters to have on a page in order to preserve work boundaries (default: 4500)
            outputCol (str): The name of the output column (default: [self.uid]_output)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setBoundaryRegex(self, value):
        """

        Args:

            boundaryRegex (str): how to split into words (default: \s)

        """
        self._set(boundaryRegex=value)
        return self


    def getBoundaryRegex(self):
        """

        Returns:

            str: how to split into words (default: \s)
        """
        return self.getOrDefault(self.boundaryRegex)


    def setInputCol(self, value):
        """

        Args:

            inputCol (str): The name of the input column

        """
        self._set(inputCol=value)
        return self


    def getInputCol(self):
        """

        Returns:

            str: The name of the input column
        """
        return self.getOrDefault(self.inputCol)


    def setMaximumPageLength(self, value):
        """

        Args:

            maximumPageLength (int): the maximum number of characters to be in a page (default: 5000)

        """
        self._set(maximumPageLength=value)
        return self


    def getMaximumPageLength(self):
        """

        Returns:

            int: the maximum number of characters to be in a page (default: 5000)
        """
        return self.getOrDefault(self.maximumPageLength)


    def setMinimumPageLength(self, value):
        """

        Args:

            minimumPageLength (int): the the minimum number of characters to have on a page in order to preserve work boundaries (default: 4500)

        """
        self._set(minimumPageLength=value)
        return self


    def getMinimumPageLength(self):
        """

        Returns:

            int: the the minimum number of characters to have on a page in order to preserve work boundaries (default: 4500)
        """
        return self.getOrDefault(self.minimumPageLength)


    def setOutputCol(self, value):
        """

        Args:

            outputCol (str): The name of the output column (default: [self.uid]_output)

        """
        self._set(outputCol=value)
        return self


    def getOutputCol(self):
        """

        Returns:

            str: The name of the output column (default: [self.uid]_output)
        """
        return self.getOrDefault(self.outputCol)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.PageSplitter"

    @staticmethod
    def _from_java(java_stage):
        module_name=PageSplitter.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".PageSplitter"
        return from_java(java_stage, module_name)
