from pyspark.sql import DataFrame
from mmlspark.explainers.HasBackgroundData import HasBackgroundData
from mmlspark.explainers._TabularSHAP import _TabularSHAP
from pyspark.ml.common import inherit_doc
from pyspark import keyword_only


@inherit_doc
class TabularSHAP(_TabularSHAP, HasBackgroundData):
    pass
