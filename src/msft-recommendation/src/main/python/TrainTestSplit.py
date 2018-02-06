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

import numpy as np


# Methods for train-test split.

class TrainTestSplit:
    '''
    Methods in this class split an input Spark DataFrame of rating tuples into train and test DataFrames for different testing purposes.
    '''

    def __init__(self, rating, by_customer=True, customerCol="customer", itemCol="item", ratingCol="rating"):
        '''
        Initialize a splitter for training and testing.
        :param rating: input Spark DataFrame for rating data.
        :param by_customer: splitting is performed by customer, otherwise by item (if False).
        '''
        self.rating = rating
        self.customerCol = customerCol
        self.itemCol = itemCol
        self.ratingCol = ratingCol
        self.by_customer = by_customer

        if by_customer:
            self.split_by_column = customerCol
            self.split_with_column = itemCol
        else:
            self.split_by_column = itemCol
            self.split_with_column = customerCol

    def min_rating_filter(self, min_rating):
        '''
        Filter rating DataFrame for each user with minimum rating.
        :param min_rating: minimum number of rating for filtering.
        :param by: by which variable (customer or item) to filter the rating.
        '''
        from pyspark.sql.functions import col

        rating_filtered = self.rating\
            .groupBy(self.split_by_column) \
            .agg({self.split_with_column: "count"}) \
            .withColumnRenamed('count(' + self.split_with_column + ')', "n" + self.split_with_column) \
            .where(col("n" + self.split_with_column) >= min_rating) \
            .join(self.rating, self.split_by_column) \
            .drop("n" + self.split_with_column)

        return (rating_filtered)

    def stratified_split(self, min_rating, ratio=0.3, fixed_test_sample=False, sample=3):
        '''
        Perform stratified sampling on rating DataFrame to split into train and test.
        :param min_rating: minimum number of rating for filtering.
        :param ratio: splitting ratio for train and test.
        :param by: by which variable (customer or item) to filter the rating.
        '''
        from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
        from pyspark.sql import SparkSession

        rating_joined = self.min_rating_filter(min_rating)

        rating_stratified_rdd = rating_joined.groupBy(self.split_by_column) \
            .agg({self.split_with_column: "count"}) \
            .withColumnRenamed('count(' + self.split_with_column + ')', 'n' + self.split_with_column) \
            .rdd

        perm_indices = rating_stratified_rdd.map(lambda r: (r[0], np.random.permutation(r[1]), r[1]))
        perm_indices.cache()

        if not fixed_test_sample:
            tr_idx = perm_indices.map(lambda r: (r[0], r[1][: int(round(r[2] * ratio))]))
            test_idx = perm_indices.map(lambda r: (r[0], r[1][int(round(r[2] * ratio)):]))
        else:
            tr_idx = perm_indices.map(lambda r: (r[0], r[1][: int(r[2] - sample)]))
            test_idx = perm_indices.map(lambda r: (r[0], r[1][int(r[2] - sample):]))

        if self.by_customer:
            tr_by = rating_joined.rdd.groupBy(lambda r: r[0]).join(tr_idx).flatMap(
                lambda r: np.array([x for x in r[1][0]])[r[1][1]])
            test_by = rating_joined.rdd.groupBy(lambda r: r[0]).join(test_idx).flatMap(
                lambda r: np.array([x for x in r[1][0]])[r[1][1]])
        else:
            tr_by = rating_joined.rdd.groupBy(lambda r: r[1]).join(tr_idx).flatMap(
                lambda r: np.array([x for x in r[1][0]])[r[1][1]])
            test_by = rating_joined.rdd.groupBy(lambda r: r[1]).join(test_idx).flatMap(
                lambda r: np.array([x for x in r[1][0]])[r[1][1]])

        rating_train = tr_by.map(lambda r: (int(r[0]), int(r[1]), float(r[2])))
        rating_test = test_by.map(lambda r: (int(r[0]), int(r[1]), float(r[2])))

        # Return DataFrame instead of RDD. NOTE this will affect rating_training of model (depending on mllib or ml being used).

        schema = StructType([
            StructField(self.customerCol, IntegerType(), True),
            StructField(self.itemCol, IntegerType(), True),
            StructField(self.ratingCol, DoubleType(), True)
        ])

        rating_train = SparkSession.builder.getOrCreate().createDataFrame(rating_train, schema)
        rating_test = SparkSession.builder.getOrCreate().createDataFrame(rating_test, schema)

        return rating_train, rating_test

    def chronological_split(self, min_rating, ratio=0.3, fixed_test_sample=False, sample=3):
        '''
        Chronological splitting split data (items are ordered by timestamps for each customer) by timestamps. Fixed ratio and fixed number also apply to splitting.
        :param min_rating: minimum number of rating for filtering.
        :param ratio: sampling ratio for testing set .
        :param fixed_test_sample: whether or not fixing the number in sampling testing data.
        :param sample: number of samples for testing data.
        '''
        from pyspark.sql import Window
        from pyspark.sql.functions import col, bround, row_number

        # Check if there is timestamp availabe in the columns.

        rating_joined = self.min_rating_filter(min_rating)

        rating_grouped = rating_joined \
            .groupBy(self.split_by_column) \
            .agg({'timeStamp': 'count'}) \
            .withColumnRenamed('count(timeStamp)', 'count')

        window_spec = Window.partitionBy(self.split_by_column).orderBy(col('timeStamp').desc())

        if fixed_test_sample == False:
            rating_all = rating_joined.join(rating_grouped, on=self.split_by_column, how="outer") \
                .withColumn('splitPoint', bround(col('count') * ratio))

            rating_train = rating_all \
                .select('*', row_number().over(window_spec).alias('rank')) \
                .filter(col('rank') > col('splitPoint'))
            rating_test = rating_all \
                .select('*', row_number().over(window_spec).alias('rank')) \
                .filter(col('rank') <= col('splitPoint'))
        else:
            rating_train = rating_joined \
                .select('*', row_number().over(window_spec).alias('rank')) \
                .filter(col('rank') > sample)
            rating_test = rating_joined \
                .select('*', row_number().over(window_spec).alias('rank')) \
                .filter(col('rank') <= sample)

        rating_train = rating_train.select(self.split_by_column, self.split_with_column, "timeStamp")
        rating_test = rating_test.select(self.split_by_column, self.split_with_column, "timeStamp")

        return rating_train, rating_test

    def non_overlapping_split(self, min_rating, ratio=0.7, fixed_test_sample=False, sample=3):
        '''
        Split by customer or item. Customer (or item) in sets of training and testing data are mutually exclusive.
        This is useful if one needs to split train and test sets for evaluating cold-start situations. 
        :param min_rating: minimum number of rating for filtering.
        :param ratio: sampling ratio for testing set .
        :param fixed_test_sample: whether or not fixing the number in sampling testing data.
        :param sample: number of samples for testing data.
        '''
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number, col, rand

        rating_joined = self.min_rating_filter(min_rating)

        rating_exclusive = rating_joined.groupBy(self.split_by_column) \
            .agg({self.split_with_column: "count"}) \
            .withColumnRenamed("count(" + self.split_with_column + ")", "n" + self.split_with_column) \
            .drop("n" + self.split_with_column)

        if not fixed_test_sample:
            rating_split = rating_exclusive.randomSplit([ratio, 1 - ratio])
        else:
            count = rating_exclusive.count()

            window_spec = Window.orderBy(rand())
            rating_tmp = rating_exclusive.select(col("*"), row_number().over(window_spec).alias("rowNumber"))

            rating_split = \
                rating_tmp.filter(rating_tmp['rowNumber'] <= (count - sample)).drop("rowNumber"), \
                rating_tmp.filter(rating_tmp['rowNumber'] > (count - sample)).drop('rowNumber')

        rating_train = rating_joined.join(rating_split[0], self.split_by_column)
        rating_test = rating_joined.join(rating_split[1], self.split_by_column)

        return rating_train, rating_test

    def random_split(self, min_rating, ratio=0.7, fixed_test_sample=False, sample=3):
        '''
        Purely random splitting. This can be generally used for evaluating either ranking or rating metrics.
        :param min_rating: minimum number of rating for filtering.
        :param ratio: sampling ratio for testing set .
        :param fixed_test_sample: whether or not fixing the number in sampling testing data.
        :param sample: number of samples for testing data.
        '''
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number, col, rand

        rating_joined = self.min_rating_filter(min_rating)

        if not fixed_test_sample:
            rating_split = rating_joined.randomSplit([ratio, 1 - ratio])
        else:
            count = rating_joined.count()

            window_spec = Window.orderBy(rand())
            rating_tmp = rating_joined.select(col("*"), row_number().over(window_spec).alias("rowNumber"))

            rating_split = \
                rating_tmp.filter(rating_tmp['rowNumber'] <= (count - sample)).drop("rowNumber"), \
                rating_tmp.filter(rating_tmp['rowNumber'] > (count - sample)).drop('rowNumber')

        rating_train = rating_split[0]
        rating_test = rating_split[1]

        return rating_train, rating_test


def _test():
    """
    Perform testing on the methods.
    """
    from pyspark.sql import SparkSession
    import pandas as pd

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("EvaluationTest") \
        .getOrCreate()

    # Synthesize some testing data.

    rating_true = pd.DataFrame({
        'customerID': [1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4],
        'itemID': [3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 2, 3, 4, 5, 6, 7, 2, 3, 4, 5, 6],
        'rating': [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5],
        'timeStamp': [d.strftime('%Y%m%d') for d in pd.date_range('2018-01-01', '2018-01-21')]
    })

    dfs_true = spark.createDataFrame(rating_true)

    splitter = TrainTestSplit(dfs_true, by_customer=True)

    # # Test minimum rating filtering methods.

    # splitter.min_rating_filter(6).show()

    # Stratified split.

    train, test = splitter.stratified_split(3, fixed_test_sample=False, ratio=0.5)
    train.show()
    test.show()

    train, test = splitter.stratified_split(3, fixed_test_sample=True, sample=2)
    train.show()
    test.show()

    # # chronological splitting

    # train, test = splitter.chronological_split(3, fixed_test_sample=False, ratio=0.3)
    # train.show()
    # test.show()

    # train, test = splitter.chronological_split(3, fixed_test_sample=True, sample=3)
    # train.show()
    # test.show()

    # # non-overlapping splitting

    # train, test = splitter.non_overlapping_split(3, fixed_test_sample=False, ratio=0.5)
    # train.show()
    # test.show()

    # train, test = splitter.non_overlapping_split(3, fixed_test_sample=True, sample=3)
    # train.show()
    # test.show()

    # # random splitting

    # train, test = splitter.random_split(3, fixed_test_sample=False, ratio=0.5)
    # train.show()
    # test.show()

    # train, test = splitter.random_split(3, fixed_test_sample=True, sample=3)
    # train.show()
    # test.show()


if __name__ == "__main__":
    _test()
