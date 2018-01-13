from pyspark.ml.wrapper import JavaWrapper
from pyspark.ml.param import Param, Params
from pyspark.ml.param.shared import HasLabelCol, HasPredictionCol, HasRawPredictionCol
from pyspark.ml.util import keyword_only
from pyspark.mllib.common import inherit_doc


class MsftRecommendationEvaluator(JavaEvaluator, HasLabelCol, HasPredictionCol):
    @keyword_only
    def __init__(self, rawPredictionCol="rawPrediction", labelCol="label",
                 metricName="ndcgAt"):
        """
        __init__(self, rawPredictionCol="rawPrediction", labelCol="label", \
                 metricName="ndcgAt")
        """
        super(MsftRecommendationEvaluator, self).__init__()
        self._java_obj = self._new_java_obj(
            "com.microsoft.ml.spark.MsftRecommendationEvaluator", self.uid)
        self._setDefault(metricName="ndcgAt")
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def setMetricName(self, value):
        """
        Sets the value of :py:attr:`metricName`.
        """
        return self._set(metricName=value)

    def getMetricName(self):
        """
        Gets the value of metricName or its default value.
        """
        return self.getOrDefault(self.metricName)

    def setK(self, value):
        """
        Sets the value of :py:attr:`metricName`.
        """
        return self._set(k=value)

    def getK(self):
        """
        Gets the value of metricName or its default value.
        """
        return self.getOrDefault(self.k)

    @keyword_only
    def setParams(self, rawPredictionCol="rawPrediction", labelCol="label",
                  metricName="ndcgAt"):
        """
        setParams(self, rawPredictionCol="rawPrediction", labelCol="label", \
                  metricName="areaUnderROC")
        Sets params for binary classification evaluator.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)


