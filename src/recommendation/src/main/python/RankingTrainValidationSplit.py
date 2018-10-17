# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import pyspark
import sys
from mmlspark.TrainTestSplit import *
from pyspark.ml import Estimator
from pyspark.ml.param import Params, Param, TypeConverters
from pyspark.ml.util import *
from pyspark.ml.common import inherit_doc
from pyspark.ml import Model
from pyspark.ml.util import *
from mmlspark._RankingTrainValidationSplit import _RankingTrainValidationSplit

if sys.version >= '3':
    basestring = str

@inherit_doc
class RankingTrainValidationSplit(_RankingTrainValidationSplit):
    """


    Args:

        collectSubMetrics (bool):
        collectSubModels (bool): whether to collect a list of sub-models trained during tuning. If set to false, then only the single best sub-model will be available after fitting. If set to true, then all sub-models will be available. Warning: For large models, collecting all sub-models can cause OOMs on the Spark driver (default: false)
        estimator (str): estimator for selection
        estimatorParamMaps (str): param maps for the estimator
        evaluator (str): evaluator used to select hyper-parameters that maximize the validated metric
        itemCol (str): Column of items
        minRatingsPerItem (int): min ratings for items > 0 (default: 1)
        minRatingsPerUser (int): min ratings for users > 0 (default: 1)
        parallelism (int): the number of threads to use when running parallel algorithms (default: 1)
        ratingCol (str): Column of ratings
        seed (long): random seed (default: 1571696510)
        trainRatio (double): ratio between training set and validation set (>= 0 && <= 1) (default: 0.75)
        userCol (str): Column of users
    """

    @keyword_only
    def __init__(self, collectSubMetrics=None, collectSubModels=False, estimator=None, estimatorParamMaps=None,
                 evaluator=None, itemCol=None, minRatingsPerItem=1, minRatingsPerUser=1, parallelism=1, ratingCol=None,
                 seed=1571696510, trainRatio=0.75, userCol=None):
        super(RankingTrainValidationSplit, self).__init__()
        if hasattr(self, "_input_kwargs"):
            kwargs = self._input_kwargs
        else:
            kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)
