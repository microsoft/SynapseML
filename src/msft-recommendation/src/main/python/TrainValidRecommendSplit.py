# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import sys

if sys.version >= '3':
    basestring = str

import numpy as np

from pyspark.sql import Window

from pyspark.ml.param.shared import *
from pyspark.ml.common import inherit_doc
from pyspark.ml.util import *
from pyspark.sql.functions import col, expr
import pyspark.sql.functions as F
from mmlspark._TrainValidRecommendSplit import _TrainValidRecommendSplit as tv

from mmlspark.Utils import *
from mmlspark.TrainTestSplit import *


@inherit_doc
class TrainValidRecommendSplit(tv):

    def _fit(self, dataset):
        rating = self.getOrDefault(self.ratingCol)
        est = self.getOrDefault(self.estimator)
        eva = self.getOrDefault(self.evaluator)
        epm = self.getOrDefault(self.estimatorParamMaps)
        num_models = len(epm)
        metrics = [0.0] * num_models

        customerID = self.getOrDefault(self.userCol)
        itemID = self.getOrDefault(self.itemCol)

        def filter_ratings(dataset):
            minRatingsU = 4

            tmpDF = dataset \
                .groupBy(customerID) \
                .agg({itemID: "count"}) \
                .withColumnRenamed('count(' + itemID + ')', 'nitems') \
                .where(col('nitems') >= minRatingsU)

            minRatingsI = 4

            tmp_df2 = dataset \
                .groupBy(itemID) \
                .agg({customerID: "count"}) \
                .withColumnRenamed('count(' + customerID + ')', 'ncustomers') \
                .where(col('ncustomers') >= minRatingsI)

            input_df = tmp_df2 \
                .join(dataset, itemID) \
                .drop('ncustomers') \
                .join(tmpDF, customerID) \
                .drop('nitems')

            return input_df

        print("filter dataset")
        filtered_dataset = filter_ratings(dataset.dropDuplicates())

        def split_df(input_df):
            # Stratified sampling by item into a train and test data set
            nusers_by_item = input_df \
                .groupBy(itemID) \
                .agg({customerID: "count"}) \
                .withColumnRenamed('count(' + customerID + ')', 'nusers').rdd

            perm_indices = nusers_by_item.map(lambda r: (r[0], np.random.permutation(r[1]), r[1]))

            RATIO = 0.75
            tr_idx = perm_indices.map(lambda r: (r[0], r[1][: int(round(r[2] * RATIO))]))
            test_idx = perm_indices.map(lambda r: (r[0], r[1][int(round(r[2] * RATIO)):]))

            tr_by_item = input_df.rdd \
                .groupBy(lambda r: r[1]) \
                .join(tr_idx) \
                .flatMap(lambda r: [x for x in r[1][0]][: len(r[1][1])])

            test_by_item = input_df.rdd \
                .groupBy(lambda r: r[1]) \
                .join(test_idx) \
                .flatMap(lambda r: [x for x in r[1][0]][: len(r[1][1])])

            train_temp = tr_by_item \
                .map(lambda r: tuple(r)) \
                .toDF(schema=[customerID, itemID, customerID + 'Org', itemID + 'Org', rating])

            test_temp = test_by_item \
                .map(lambda r: tuple(r)) \
                .toDF(schema=[customerID, itemID, customerID + 'Org', itemID + 'Org', rating])

            return [train_temp, test_temp]

        print("split dataset")
        #[train, validation] = self._call_java("splitDF", filtered_dataset)

        train, validation = TrainTestSplit.stratified_split(filtered_dataset)
        # [train, validation] = split_df(filtered_dataset)
        train.cache()
        validation.cache()
        eva.setNumberItems(validation.rdd.map(lambda r: r[1]).distinct().count())
        print("fit starting")
        models = est.fit(train, epm)
        train.unpersist()

        def prepare_test_data(dataset, recs, k):

            userColumn = est.getUserCol()
            itemColumn = est.getItemCol()

            perUserRecommendedItemsDF = recs \
                .select(userColumn, "recommendations." + itemColumn) \
                .withColumnRenamed(itemColumn, 'prediction')

            windowSpec = Window.partitionBy(userColumn).orderBy(col(rating).desc())

            perUserActualItemsDF = dataset \
                .select(userColumn, itemColumn, rating, F.rank().over(windowSpec).alias('rank')) \
                .where('rank <= {0}'.format(k)) \
                .groupBy(userColumn) \
                .agg(expr('collect_list(' + itemColumn + ') as label')) \
                .select(userColumn, "label")

            joined_rec_actual = perUserRecommendedItemsDF \
                .join(perUserActualItemsDF, on=userColumn) \
                .drop(userColumn)

            return joined_rec_actual

        def cast_model(model):
            recs = model._call_java("recommendForAllUsers", eva.getK())
            prepared_test = prepare_test_data(model.transform(validation), recs, eva.getK())
            metric = eva.evaluate(prepared_test)
            return metric

        for j in range(num_models):
            metrics[j] += cast_model(models[j])
        validation.unpersist()

        if eva.isLargerBetter():
            best_index = np.argmax(metrics)
        else:
            best_index = np.argmin(metrics)
        best_model = est.fit(dataset, epm[best_index])
        return self._copyValues(TrainValidRecommendSplitModel(best_model, metrics))
