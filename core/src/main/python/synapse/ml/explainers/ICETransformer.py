from synapse.ml.explainers._ICETransformer import _ICETransformer
from pyspark.ml.common import inherit_doc
from pyspark import SparkContext

@inherit_doc
class ICETransformer(_ICETransformer):
    def setCategoricalFeatures(self, value):
        """
        Args:
        categoricalFeatures: The list of categorical features to explain.
        """
        sc = SparkContext._active_spark_context
        feature_list = [v.getObject() for v in value]
        return super().setCategoricalFeatures(sc._jvm.PythonUtils.toSeq(feature_list))

    def setNumericFeatures(self, value):
        """
        Args:
        categoricalFeatures: The list of categorical features to explain.
        """
        sc = SparkContext._active_spark_context
        feature_list = [v.getObject() for v in value]
        return super().setNumericFeatures(sc._jvm.PythonUtils.toSeq(feature_list))
