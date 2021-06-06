from mmlspark.explainers.HasBackgroundData import HasBackgroundData
from mmlspark.explainers._VectorLIME import _VectorLIME
from pyspark.ml.common import inherit_doc


@inherit_doc
class VectorLIME(_VectorLIME, HasBackgroundData):
    pass
