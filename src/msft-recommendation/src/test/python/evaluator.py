# Copyright (C) Microsoft Corporation.
#
# Microsoft Corporation ("Microsoft") grants you a nonexclusive, perpetual, royalty-free right to use, 
# copy, and modify the software code provided by us ("Software Code"). You may not sublicense the 
# Software Code or any use of it (except to your affiliates and to vendors to perform work on your behalf)
# through distribution, network access, service agreement, lease, rental, or otherwise. This license does
# not purport to express any claim of ownership over data you may have shared with Microsoft in the creation
# of the Software Code. Unless applicable law gives you more rights, Microsoft reserves all other rights not 
# expressly granted herein, whether by implication, estoppel or otherwise.
#
# THE SOFTWARE CODE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
# LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO 
# EVENT SHALL MICROSOFT OR ITS LICENSORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
# OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS 
# OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
# IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE 
# USE OF THE SAMPLE CODE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# Prepare training and test data.

import numpy as np
import pandas as pd
import pyspark
import unittest
from mmlspark.evaluate import *
from pyspark.ml.tuning import *
from pyspark.sql.types import *


class EvaluationSpec(unittest.TestCase):
    k = 5

    def test_ranking_metrics(self):
        '''
        Test ranking evaluation methods
        '''
        dfs_pred, dfs_true, rating_pred, spark = self.create_sample_data()

        # Evaluate ranking metrics.

        evaluator_ranking = RankingEvaluation(self.k, dfs_true, dfs_pred)

        recall = evaluator_ranking.recall_at_k()
        precision = evaluator_ranking.precision_at_k()
        ndcg = evaluator_ranking.ndcg_at_k()
        map = evaluator_ranking.map_at_k()

        self.assertTrue(isinstance(recall, float) & (recall <= 1))
        self.assertTrue(isinstance(precision, float) & (precision <= 1))
        self.assertTrue(isinstance(ndcg, float) & (ndcg <= 1))
        self.assertTrue(isinstance(map, float) & (map <= 1))

    def test_distribution_metrics(self):
        '''
        Test ranking evaluation methods
        '''
        dfs_pred, dfs_true, rating_pred, spark = self.create_sample_data()

        # Evaluate distribution metrics.

        evaluator_distribution = DistributionMetrics(self.k, dfs_true, dfs_pred)

        self.assertTrue(bool(evaluator_distribution.popularity_at_k().head(1)))

        diversity = evaluator_distribution.diversity_at_k()
        max_diversity = evaluator_distribution.max_diversity()

        self.assertTrue(diversity <= max_diversity)
        self.assertTrue((diversity <= 1))  # isinstance(diversity, float) &
        self.assertTrue((max_diversity <= 1))  # isinstance(max_diversity, float) &

    def test_rating_metrics(self):
        '''
        Test ranking evaluation methods
        '''
        dfs_pred, dfs_true, rating_pred, spark = self.create_sample_data()

        # Evaluate rating metrics.

        rating_pred['rating'] = rating_pred['rating'] + np.random.normal(0, 1, rating_pred.shape[0])

        dfs_pred = spark.createDataFrame(rating_pred)

        evaluator_rating = RatingEvaluation(dfs_true, dfs_pred)

        rsquared = evaluator_rating.rsquared
        exp_var = evaluator_rating.exp_var
        mae = evaluator_rating.mae
        rmse = evaluator_rating.rmse

        self.assertTrue(isinstance(rsquared, float))
        self.assertTrue(isinstance(exp_var, float))
        self.assertTrue(isinstance(mae, float))
        self.assertTrue(isinstance(rmse, float))

    @staticmethod
    def create_sample_data():
        spark = pyspark.sql.SparkSession.builder.master("local[*]") \
            .config('spark.driver.extraClassPath',
                    "/home/dciborow/mmlspark2/BuildArtifacts/packages/m2/com/microsoft/ml/spark/mmlspark_2.11/0.0/mmlspark_2.11-0.0.jar") \
            .getOrCreate()
        # Synthesize some testing data.
        rating_pred = pd.DataFrame({
            'customerID': [1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4],
            'itemID': [1, 2, 3, 4, 5, 2, 3, 4, 5, 6, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5],
            'rating': [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5],
            'timeStamp': [d.strftime('%Y%m%d') for d in pd.date_range('2018-01-01', '2018-01-21')]
        })
        rating_true = pd.DataFrame({
            'customerID': [1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4],
            'itemID': [3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 2, 3, 4, 5, 6, 7, 2, 3, 4, 5, 6],
            'rating': [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5],
            'timeStamp': [d.strftime('%Y%m%d') for d in pd.date_range('2018-01-01', '2018-01-21')]
        })
        dfs_pred = spark.createDataFrame(rating_pred)
        dfs_true = spark.createDataFrame(rating_true)
        return dfs_pred, dfs_true, rating_pred, spark


if __name__ == '__main__':
    unittest.main(exit=False)
