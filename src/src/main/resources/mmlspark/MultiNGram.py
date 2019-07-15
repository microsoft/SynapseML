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
class MultiNGram(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        inputCol (str): The name of the input column
        lengths (object): the collection of lengths to use for ngram extraction
        outputCol (str): The name of the output column (default: [self.uid]_output)
    """

    @keyword_only
    def __init__(self, inputCol=None, lengths=None, outputCol=None):
        super(MultiNGram, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.MultiNGram")
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column")
        self.lengths = Param(self, "lengths", "lengths: the collection of lengths to use for ngram extraction")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column (default: [self.uid]_output)")
        self._setDefault(outputCol=self.uid + "_output")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, lengths=None, outputCol=None):
        """
        Set the (keyword only) parameters

        Args:

            inputCol (str): The name of the input column
            lengths (object): the collection of lengths to use for ngram extraction
            outputCol (str): The name of the output column (default: [self.uid]_output)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

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


    def setLengths(self, value):
        """

        Args:

            lengths (object): the collection of lengths to use for ngram extraction

        """
        self._set(lengths=value)
        return self


    def getLengths(self):
        """

        Returns:

            object: the collection of lengths to use for ngram extraction
        """
        return self.getOrDefault(self.lengths)


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
        return "com.microsoft.ml.spark.MultiNGram"

    @staticmethod
    def _from_java(java_stage):
        module_name=MultiNGram.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".MultiNGram"
        return from_java(java_stage, module_name)
