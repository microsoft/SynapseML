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
class _ResizeImageTransformer(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        height (int): the width of the image
        inputCol (str): The name of the input column (default: image)
        nChannels (int): the number of channels of the target image
        outputCol (str): The name of the output column (default: [self.uid]_output)
        width (int): the width of the image
    """

    @keyword_only
    def __init__(self, height=None, inputCol="image", nChannels=None, outputCol=None, width=None):
        super(_ResizeImageTransformer, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.ResizeImageTransformer")
        self.height = Param(self, "height", "height: the width of the image")
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column (default: image)")
        self._setDefault(inputCol="image")
        self.nChannels = Param(self, "nChannels", "nChannels: the number of channels of the target image")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column (default: [self.uid]_output)")
        self._setDefault(outputCol=self.uid + "_output")
        self.width = Param(self, "width", "width: the width of the image")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, height=None, inputCol="image", nChannels=None, outputCol=None, width=None):
        """
        Set the (keyword only) parameters

        Args:

            height (int): the width of the image
            inputCol (str): The name of the input column (default: image)
            nChannels (int): the number of channels of the target image
            outputCol (str): The name of the output column (default: [self.uid]_output)
            width (int): the width of the image
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setHeight(self, value):
        """

        Args:

            height (int): the width of the image

        """
        self._set(height=value)
        return self


    def getHeight(self):
        """

        Returns:

            int: the width of the image
        """
        return self.getOrDefault(self.height)


    def setInputCol(self, value):
        """

        Args:

            inputCol (str): The name of the input column (default: image)

        """
        self._set(inputCol=value)
        return self


    def getInputCol(self):
        """

        Returns:

            str: The name of the input column (default: image)
        """
        return self.getOrDefault(self.inputCol)


    def setNChannels(self, value):
        """

        Args:

            nChannels (int): the number of channels of the target image

        """
        self._set(nChannels=value)
        return self


    def getNChannels(self):
        """

        Returns:

            int: the number of channels of the target image
        """
        return self.getOrDefault(self.nChannels)


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


    def setWidth(self, value):
        """

        Args:

            width (int): the width of the image

        """
        self._set(width=value)
        return self


    def getWidth(self):
        """

        Returns:

            int: the width of the image
        """
        return self.getOrDefault(self.width)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.ResizeImageTransformer"

    @staticmethod
    def _from_java(java_stage):
        module_name=_ResizeImageTransformer.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".ResizeImageTransformer"
        return from_java(java_stage, module_name)
