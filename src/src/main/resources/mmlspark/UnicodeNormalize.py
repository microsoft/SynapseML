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
class UnicodeNormalize(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        form (str): Unicode normalization form: NFC, NFD, NFKC, NFKD
        inputCol (str): The name of the input column
        lower (bool): Lowercase text
        outputCol (str): The name of the output column
    """

    @keyword_only
    def __init__(self, form=None, inputCol=None, lower=None, outputCol=None):
        super(UnicodeNormalize, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.UnicodeNormalize")
        self.form = Param(self, "form", "form: Unicode normalization form: NFC, NFD, NFKC, NFKD")
        self.inputCol = Param(self, "inputCol", "inputCol: The name of the input column")
        self.lower = Param(self, "lower", "lower: Lowercase text")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, form=None, inputCol=None, lower=None, outputCol=None):
        """
        Set the (keyword only) parameters

        Args:

            form (str): Unicode normalization form: NFC, NFD, NFKC, NFKD
            inputCol (str): The name of the input column
            lower (bool): Lowercase text
            outputCol (str): The name of the output column
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setForm(self, value):
        """

        Args:

            form (str): Unicode normalization form: NFC, NFD, NFKC, NFKD

        """
        self._set(form=value)
        return self


    def getForm(self):
        """

        Returns:

            str: Unicode normalization form: NFC, NFD, NFKC, NFKD
        """
        return self.getOrDefault(self.form)


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


    def setLower(self, value):
        """

        Args:

            lower (bool): Lowercase text

        """
        self._set(lower=value)
        return self


    def getLower(self):
        """

        Returns:

            bool: Lowercase text
        """
        return self.getOrDefault(self.lower)


    def setOutputCol(self, value):
        """

        Args:

            outputCol (str): The name of the output column

        """
        self._set(outputCol=value)
        return self


    def getOutputCol(self):
        """

        Returns:

            str: The name of the output column
        """
        return self.getOrDefault(self.outputCol)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.UnicodeNormalize"

    @staticmethod
    def _from_java(java_stage):
        module_name=UnicodeNormalize.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".UnicodeNormalize"
        return from_java(java_stage, module_name)
