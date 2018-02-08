# coding=utf-8
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

import pandas as pd
import pyspark
import unittest
from mmlspark.TrainTestSplit import *
from mmlspark.evaluate import *
from pyspark.ml.tuning import *
from pyspark.sql.functions import col
from pyspark.sql.types import *


class SplitSpec(unittest.TestCase):

    @staticmethod
    def getDF():
        from pyspark.sql import SparkSession

        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("EvaluationTest") \
            .getOrCreate()

        # Synthesize some testing data.

        df_rating = pd.DataFrame({
            'customerID': [1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4],
            'itemID': [3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 2, 3, 4, 5, 6, 7, 2, 3, 4, 5, 6],
            'rating': [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5],
            'timeStamp': [d.strftime('%Y%m%d') for d in pd.date_range('2018-01-01', '2018-01-21')]
        })

        return spark.createDataFrame(df_rating)

    def test_filter(self):
        dfs_rating = self.getDF()
        pyspark.sql.DataFrame.min_rating_filter = TrainTestSplit.min_rating_filter

        self.assertEqual(dfs_rating.min_rating_filter(min_rating=6, by_customer=True).count(), 6)

    def test_stratified(self):
        dfs_rating = self.getDF()

        pyspark.sql.DataFrame.stratified_split = TrainTestSplit.stratified_split
        dfs_train, dfs_test = dfs_rating.stratified_split(min_rating=3, by_customer=True, fixed_test_sample=False,
                                                          ratio=0.5)
        self.assertTrue(set(dfs_train.select(col('customerID')).distinct().collect()) == set(
            dfs_test.select(col('customerID')).distinct().collect()))

        dfs_train, dfs_test = dfs_rating.stratified_split(min_rating=3, by_customer=True, fixed_test_sample=True,
                                                          sample=2)
        dfs_train.show()
        dfs_test.show()

    def test_chronological_split(self):
        dfs_rating = self.getDF()
        pyspark.sql.DataFrame.chronological_split = TrainTestSplit.chronological_split

        dfs_train, dfs_test = dfs_rating.chronological_split(min_rating=3, by_customer=True, fixed_test_sample=False,
                                                             ratio=0.3)
        dfs_train.show()
        dfs_test.show()

        dfs_train, dfs_test = dfs_rating.chronological_split(min_rating=3, by_customer=True, fixed_test_sample=True,
                                                             sample=3)
        dfs_train.show()
        dfs_test.show()

    def test_non_overlapping_split(self):
        dfs_rating = self.getDF()
        pyspark.sql.DataFrame.non_overlapping_split = TrainTestSplit.non_overlapping_split

        dfs_train, dfs_test = dfs_rating.non_overlapping_split(min_rating=3, by_customer=True, fixed_test_sample=False,
                                                               ratio=0.5)
        dfs_train.show()
        dfs_test.show()

        dfs_train, dfs_test = dfs_rating.non_overlapping_split(min_rating=3, by_customer=True, fixed_test_sample=True,
                                                               sample=3)
        dfs_train.show()
        dfs_test.show()

    def test_random_split(self):
        dfs_rating = self.getDF()
        pyspark.sql.DataFrame.random_split = TrainTestSplit.random_split

        dfs_train, dfs_test = dfs_rating.random_split(min_rating=3, by_customer=True, fixed_test_sample=False,
                                                      ratio=0.5)
        dfs_train.show()
        dfs_test.show()

        dfs_train, dfs_test = dfs_rating.random_split(min_rating=3, by_customer=True, fixed_test_sample=True, sample=3)
        dfs_train.show()
        dfs_test.show()


if __name__ == '__main__':
    unittest.main(exit=False)
