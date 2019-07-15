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
from mmlspark.TypeConversionUtils import generateTypeConverter, complexTypeConverter

@inherit_doc
class DetectAnomalies(ComplexParamsMixin, JavaMLReadable, JavaMLWritable, JavaTransformer):
    """


    Args:

        concurrency (int): max number of concurrent calls (default: 1)
        concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
        customInterval (object): Custom Interval is used to set non-standard time interval, for example, if the series is 5 minutes, request can be set as granularity=minutely, customInterval=5.
        errorCol (str): column to hold http errors (default: [self.uid]_error)
        granularity (object): Can only be one of yearly, monthly, weekly, daily, hourly or minutely.Granularity is used for verify whether input series is valid.
        handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
        maxAnomalyRatio (object): Optional argument, advanced model parameter, max anomaly ratio in a time series.
        outputCol (str): The name of the output column (default: [self.uid]_output)
        period (object): Optional argument, periodic value of a time series.If the value is null or does not present, the API will determine the period automatically.
        sensitivity (object): Optional argument, advanced model parameter, between 0-99,the lower the value is, the larger the margin value will be which means less anomalies will be accepted
        series (object): Time series data points. Points should be sorted by timestamp in ascending orderto match the anomaly detection result. If the data is not sorted correctly orthere is duplicated timestamp, the API will not work.In such case, an error message will be returned.
        subscriptionKey (object): the API key to use
        timeout (double): number of seconds to wait before closing the connection (default: 60.0)
        url (str): Url of the service
    """

    @keyword_only
    def __init__(self, concurrency=1, concurrentTimeout=100.0, customInterval=None, errorCol=None, granularity=None, handler=None, maxAnomalyRatio=None, outputCol=None, period=None, sensitivity=None, series=None, subscriptionKey=None, timeout=60.0, url=None):
        super(DetectAnomalies, self).__init__()
        self._java_obj = self._new_java_obj("com.microsoft.ml.spark.DetectAnomalies")
        self._cache = {}
        self.concurrency = Param(self, "concurrency", "concurrency: max number of concurrent calls (default: 1)")
        self._setDefault(concurrency=1)
        self.concurrentTimeout = Param(self, "concurrentTimeout", "concurrentTimeout: max number seconds to wait on futures if concurrency >= 1 (default: 100.0)")
        self._setDefault(concurrentTimeout=100.0)
        self.customInterval = Param(self, "customInterval", "customInterval: Custom Interval is used to set non-standard time interval, for example, if the series is 5 minutes, request can be set as granularity=minutely, customInterval=5.")
        self.errorCol = Param(self, "errorCol", "errorCol: column to hold http errors (default: [self.uid]_error)")
        self._setDefault(errorCol=self.uid + "_error")
        self.granularity = Param(self, "granularity", "granularity: Can only be one of yearly, monthly, weekly, daily, hourly or minutely.Granularity is used for verify whether input series is valid.")
        self.handler = Param(self, "handler", "handler: Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))", generateTypeConverter("handler", self._cache, complexTypeConverter))
        self.maxAnomalyRatio = Param(self, "maxAnomalyRatio", "maxAnomalyRatio: Optional argument, advanced model parameter, max anomaly ratio in a time series.")
        self.outputCol = Param(self, "outputCol", "outputCol: The name of the output column (default: [self.uid]_output)")
        self._setDefault(outputCol=self.uid + "_output")
        self.period = Param(self, "period", "period: Optional argument, periodic value of a time series.If the value is null or does not present, the API will determine the period automatically.")
        self.sensitivity = Param(self, "sensitivity", "sensitivity: Optional argument, advanced model parameter, between 0-99,the lower the value is, the larger the margin value will be which means less anomalies will be accepted")
        self.series = Param(self, "series", "series: Time series data points. Points should be sorted by timestamp in ascending orderto match the anomaly detection result. If the data is not sorted correctly orthere is duplicated timestamp, the API will not work.In such case, an error message will be returned.")
        self.subscriptionKey = Param(self, "subscriptionKey", "subscriptionKey: the API key to use")
        self.timeout = Param(self, "timeout", "timeout: number of seconds to wait before closing the connection (default: 60.0)")
        self._setDefault(timeout=60.0)
        self.url = Param(self, "url", "url: Url of the service")
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, concurrency=1, concurrentTimeout=100.0, customInterval=None, errorCol=None, granularity=None, handler=None, maxAnomalyRatio=None, outputCol=None, period=None, sensitivity=None, series=None, subscriptionKey=None, timeout=60.0, url=None):
        """
        Set the (keyword only) parameters

        Args:

            concurrency (int): max number of concurrent calls (default: 1)
            concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
            customInterval (object): Custom Interval is used to set non-standard time interval, for example, if the series is 5 minutes, request can be set as granularity=minutely, customInterval=5.
            errorCol (str): column to hold http errors (default: [self.uid]_error)
            granularity (object): Can only be one of yearly, monthly, weekly, daily, hourly or minutely.Granularity is used for verify whether input series is valid.
            handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
            maxAnomalyRatio (object): Optional argument, advanced model parameter, max anomaly ratio in a time series.
            outputCol (str): The name of the output column (default: [self.uid]_output)
            period (object): Optional argument, periodic value of a time series.If the value is null or does not present, the API will determine the period automatically.
            sensitivity (object): Optional argument, advanced model parameter, between 0-99,the lower the value is, the larger the margin value will be which means less anomalies will be accepted
            series (object): Time series data points. Points should be sorted by timestamp in ascending orderto match the anomaly detection result. If the data is not sorted correctly orthere is duplicated timestamp, the API will not work.In such case, an error message will be returned.
            subscriptionKey (object): the API key to use
            timeout (double): number of seconds to wait before closing the connection (default: 60.0)
            url (str): Url of the service
        """
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        return self._set(**kwargs)

    def setConcurrency(self, value):
        """

        Args:

            concurrency (int): max number of concurrent calls (default: 1)

        """
        self._set(concurrency=value)
        return self


    def getConcurrency(self):
        """

        Returns:

            int: max number of concurrent calls (default: 1)
        """
        return self.getOrDefault(self.concurrency)


    def setConcurrentTimeout(self, value):
        """

        Args:

            concurrentTimeout (double): max number seconds to wait on futures if concurrency >= 1 (default: 100.0)

        """
        self._set(concurrentTimeout=value)
        return self


    def getConcurrentTimeout(self):
        """

        Returns:

            double: max number seconds to wait on futures if concurrency >= 1 (default: 100.0)
        """
        return self.getOrDefault(self.concurrentTimeout)


    def setCustomInterval(self, value):
        """

        Args:

            customInterval (object): Custom Interval is used to set non-standard time interval, for example, if the series is 5 minutes, request can be set as granularity=minutely, customInterval=5.

        """
        self._java_obj = self._java_obj.setCustomInterval(value)
        return self


    def setCustomIntervalCol(self, value):
        """

        Args:

            customInterval (object): Custom Interval is used to set non-standard time interval, for example, if the series is 5 minutes, request can be set as granularity=minutely, customInterval=5.

        """
        self._java_obj = self._java_obj.setCustomIntervalCol(value)
        return self




    def getCustomInterval(self):
        """

        Returns:

            object: Custom Interval is used to set non-standard time interval, for example, if the series is 5 minutes, request can be set as granularity=minutely, customInterval=5.
        """
        return self._cache.get("customInterval", None)


    def setErrorCol(self, value):
        """

        Args:

            errorCol (str): column to hold http errors (default: [self.uid]_error)

        """
        self._set(errorCol=value)
        return self


    def getErrorCol(self):
        """

        Returns:

            str: column to hold http errors (default: [self.uid]_error)
        """
        return self.getOrDefault(self.errorCol)


    def setGranularity(self, value):
        """

        Args:

            granularity (object): Can only be one of yearly, monthly, weekly, daily, hourly or minutely.Granularity is used for verify whether input series is valid.

        """
        self._java_obj = self._java_obj.setGranularity(value)
        return self


    def setGranularityCol(self, value):
        """

        Args:

            granularity (object): Can only be one of yearly, monthly, weekly, daily, hourly or minutely.Granularity is used for verify whether input series is valid.

        """
        self._java_obj = self._java_obj.setGranularityCol(value)
        return self




    def getGranularity(self):
        """

        Returns:

            object: Can only be one of yearly, monthly, weekly, daily, hourly or minutely.Granularity is used for verify whether input series is valid.
        """
        return self._cache.get("granularity", None)


    def setHandler(self, value):
        """

        Args:

            handler (object): Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))

        """
        self._set(handler=value)
        return self


    def getHandler(self):
        """

        Returns:

            object: Which strategy to use when handling requests (default: UserDefinedFunction(<function2>,StringType,None))
        """
        return self._cache.get("handler", None)


    def setMaxAnomalyRatio(self, value):
        """

        Args:

            maxAnomalyRatio (object): Optional argument, advanced model parameter, max anomaly ratio in a time series.

        """
        self._java_obj = self._java_obj.setMaxAnomalyRatio(value)
        return self


    def setMaxAnomalyRatioCol(self, value):
        """

        Args:

            maxAnomalyRatio (object): Optional argument, advanced model parameter, max anomaly ratio in a time series.

        """
        self._java_obj = self._java_obj.setMaxAnomalyRatioCol(value)
        return self




    def getMaxAnomalyRatio(self):
        """

        Returns:

            object: Optional argument, advanced model parameter, max anomaly ratio in a time series.
        """
        return self._cache.get("maxAnomalyRatio", None)


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


    def setPeriod(self, value):
        """

        Args:

            period (object): Optional argument, periodic value of a time series.If the value is null or does not present, the API will determine the period automatically.

        """
        self._java_obj = self._java_obj.setPeriod(value)
        return self


    def setPeriodCol(self, value):
        """

        Args:

            period (object): Optional argument, periodic value of a time series.If the value is null or does not present, the API will determine the period automatically.

        """
        self._java_obj = self._java_obj.setPeriodCol(value)
        return self




    def getPeriod(self):
        """

        Returns:

            object: Optional argument, periodic value of a time series.If the value is null or does not present, the API will determine the period automatically.
        """
        return self._cache.get("period", None)


    def setSensitivity(self, value):
        """

        Args:

            sensitivity (object): Optional argument, advanced model parameter, between 0-99,the lower the value is, the larger the margin value will be which means less anomalies will be accepted

        """
        self._java_obj = self._java_obj.setSensitivity(value)
        return self


    def setSensitivityCol(self, value):
        """

        Args:

            sensitivity (object): Optional argument, advanced model parameter, between 0-99,the lower the value is, the larger the margin value will be which means less anomalies will be accepted

        """
        self._java_obj = self._java_obj.setSensitivityCol(value)
        return self




    def getSensitivity(self):
        """

        Returns:

            object: Optional argument, advanced model parameter, between 0-99,the lower the value is, the larger the margin value will be which means less anomalies will be accepted
        """
        return self._cache.get("sensitivity", None)


    def setSeries(self, value):
        """

        Args:

            series (object): Time series data points. Points should be sorted by timestamp in ascending orderto match the anomaly detection result. If the data is not sorted correctly orthere is duplicated timestamp, the API will not work.In such case, an error message will be returned.

        """
        self._java_obj = self._java_obj.setSeries(value)
        return self


    def setSeriesCol(self, value):
        """

        Args:

            series (object): Time series data points. Points should be sorted by timestamp in ascending orderto match the anomaly detection result. If the data is not sorted correctly orthere is duplicated timestamp, the API will not work.In such case, an error message will be returned.

        """
        self._java_obj = self._java_obj.setSeriesCol(value)
        return self




    def getSeries(self):
        """

        Returns:

            object: Time series data points. Points should be sorted by timestamp in ascending orderto match the anomaly detection result. If the data is not sorted correctly orthere is duplicated timestamp, the API will not work.In such case, an error message will be returned.
        """
        return self._cache.get("series", None)


    def setSubscriptionKey(self, value):
        """

        Args:

            subscriptionKey (object): the API key to use

        """
        self._java_obj = self._java_obj.setSubscriptionKey(value)
        return self


    def setSubscriptionKeyCol(self, value):
        """

        Args:

            subscriptionKey (object): the API key to use

        """
        self._java_obj = self._java_obj.setSubscriptionKeyCol(value)
        return self




    def getSubscriptionKey(self):
        """

        Returns:

            object: the API key to use
        """
        return self._cache.get("subscriptionKey", None)


    def setTimeout(self, value):
        """

        Args:

            timeout (double): number of seconds to wait before closing the connection (default: 60.0)

        """
        self._set(timeout=value)
        return self


    def getTimeout(self):
        """

        Returns:

            double: number of seconds to wait before closing the connection (default: 60.0)
        """
        return self.getOrDefault(self.timeout)


    def setUrl(self, value):
        """

        Args:

            url (str): Url of the service

        """
        self._set(url=value)
        return self


    def getUrl(self):
        """

        Returns:

            str: Url of the service
        """
        return self.getOrDefault(self.url)



    @classmethod
    def read(cls):
        """ Returns an MLReader instance for this class. """
        return JavaMMLReader(cls)

    @staticmethod
    def getJavaPackage():
        """ Returns package name String. """
        return "com.microsoft.ml.spark.DetectAnomalies"

    @staticmethod
    def _from_java(java_stage):
        module_name=DetectAnomalies.__module__
        module_name=module_name.rsplit(".", 1)[0] + ".DetectAnomalies"
        return from_java(java_stage, module_name)
