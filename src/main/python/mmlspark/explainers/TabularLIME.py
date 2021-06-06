from mmlspark.explainers.HasBackgroundData import HasBackgroundData
from mmlspark.explainers._TabularLIME import _TabularLIME
from pyspark.ml.common import inherit_doc


@inherit_doc
class TabularLIME(_TabularLIME, HasBackgroundData):
    pass