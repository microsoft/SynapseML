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
class SummarizeData(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """
    Compute summary statistics for the dataset.

    Statistics to be computed:

    - counts
    - basic
    - sample
    - percentiles

    errorThreshold (default 0.0) is the error threshold for quantiles.

    Args:

        basic (bool): Compute basic statistics (default: true)
        counts (bool): Compute count statistics (default: true)
        errorThreshold (double): Threshold for quantiles - 0 is exact (default: 0.0)
        percentiles (bool): Compute percentiles (default: true)
        sample (bool): Compute sample statistics (default: true)
    """

    @keyword_only
    def __init__(self, basic=True, counts=True, errorThreshold=0.0, percentiles=True, sample=True):
        super(SummarizeData, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.SummarizeData")
        self.basic = Param(self, "basic", "basic: Compute basic statistics (default: true)")
        self._setDefault(basic=True)
        self.counts = Param(self, "counts", "counts: Compute count statistics (default: true)")
        self._setDefault(counts=True)
        self.errorThreshold = Param(self, "errorThreshold", "errorThreshold: Threshold for quantiles - 0 is exact (default: 0.0)")
        self._setDefault(errorThreshold=0.0)
        self.percentiles = Param(self, "percentiles", "percentiles: Compute percentiles (default: true)")
        self._setDefault(percentiles=True)
        self.sample = Param(self, "sample", "sample: Compute sample statistics (default: true)")
        self._setDefault(sample=True)
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, basic=True, counts=True, errorThreshold=0.0, percentiles=True, sample=True):
        """
        Set the (keyword only) parameters

        Args:

            basic (bool): Compute basic statistics (default: true)
            counts (bool): Compute count statistics (default: true)
            errorThreshold (double): Threshold for quantiles - 0 is exact (default: 0.0)
            percentiles (bool): Compute percentiles (default: true)
            sample (bool): Compute sample statistics (default: true)
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setBasic(self, value):
        """

        Args:

            basic (bool): Compute basic statistics (default: true)

        """
        self._set(basic=value)
        return self


    def getBasic(self):
        """

        Returns:

            bool: Compute basic statistics (default: true)
        """
        return self.getOrDefault(self.basic)


    def setCounts(self, value):
        """

        Args:

            counts (bool): Compute count statistics (default: true)

        """
        self._set(counts=value)
        return self


    def getCounts(self):
        """

        Returns:

            bool: Compute count statistics (default: true)
        """
        return self.getOrDefault(self.counts)


    def setErrorThreshold(self, value):
        """

        Args:

            errorThreshold (double): Threshold for quantiles - 0 is exact (default: 0.0)

        """
        self._set(errorThreshold=value)
        return self


    def getErrorThreshold(self):
        """

        Returns:

            double: Threshold for quantiles - 0 is exact (default: 0.0)
        """
        return self.getOrDefault(self.errorThreshold)


    def setPercentiles(self, value):
        """

        Args:

            percentiles (bool): Compute percentiles (default: true)

        """
        self._set(percentiles=value)
        return self


    def getPercentiles(self):
        """

        Returns:

            bool: Compute percentiles (default: true)
        """
        return self.getOrDefault(self.percentiles)


    def setSample(self, value):
        """

        Args:

            sample (bool): Compute sample statistics (default: true)

        """
        self._set(sample=value)
        return self


    def getSample(self):
        """

        Returns:

            bool: Compute sample statistics (default: true)
        """
        return self.getOrDefault(self.sample)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.SummarizeData"

    @staticmethod
    def _from_java(java_stage):
        module_name=SummarizeData.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".SummarizeData"
        return from_java(java_stage, module_name)
