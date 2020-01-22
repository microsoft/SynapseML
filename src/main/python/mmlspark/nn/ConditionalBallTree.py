# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import sys
from pyspark import SparkContext

if sys.version >= '3':
    basestring = str


class ConditionalBallTree(object):

    def __init__(self, keys, values, labels, leafSize, java_obj=None):
        """
        Create a conditional ball tree.
        :param keys: 2D array representing the data, shape: n_points x n_features
        :param values: 1D array
        :param labels: 1D array
        :param leafSize: int
        """
        if java_obj is None:
            self._jconditional_balltree = SparkContext._active_spark_context._jvm \
                .com.microsoft.ml.spark.nn.ConditionalBallTree \
                .apply(keys, values, labels, leafSize)
        else:
            self._jconditional_balltree = java_obj

    def findMaximumInnerProducts(self, queryPoint, conditioner, k):
        """
        Find the best match to the queryPoint given the conditioner and k from self.
        :param queryPoint: array vector to use to query for NNs
        :param conditioner: set of labels that will subset or condition the NN query
        :param k: int representing the maximum number of neighbors to return
        :return: array of tuples representing the index of the match and its distance
        """
        return [(bm.index(), bm.distance())
                for bm in self._jconditional_balltree.findMaximumInnerProducts(queryPoint, conditioner, k)]

    def save(self, filename):
        self._jconditional_balltree.save(filename)

    @staticmethod
    def load(filename):
        java_obj = SparkContext._active_spark_context._jvm \
            .com.microsoft.ml.spark.nn.ConditionalBallTree.load(filename)
        return ConditionalBallTree(None, None, None, None, java_obj=java_obj)
