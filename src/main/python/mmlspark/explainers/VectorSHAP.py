from pyspark.sql import DataFrame
from mmlspark.explainers.HasBackgroundData import HasBackgroundData
from mmlspark.explainers._VectorSHAP import _VectorSHAP
from pyspark.ml.common import inherit_doc
from pyspark import keyword_only


@inherit_doc
class VectorSHAP(_VectorSHAP, HasBackgroundData):
    pass
