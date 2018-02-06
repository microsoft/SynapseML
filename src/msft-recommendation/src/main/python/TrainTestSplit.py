# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for information.

import numpy as np


# Methods for train-test split.
class TrainTestSplit:

    @staticmethod
    def min_rating_filter(self, min_rating, by_customer):
        '''
        Filter rating DataFrame for each user with minimum rating.
        :param by_customer:
        :param self:
        :param min_rating: minimum number of rating for filtering.
        '''
        from pyspark.sql.functions import col

        if by_customer:
            by = "customer"
            with_ = "item"
            split_by_column = by + "ID"
            split_with_column = "item" + "ID"
        else:
            by = "item"
            with_ = "customer"
            split_by_column = by + "ID"
            split_with_column = "customer" + "ID"

        rating_filtered = self.groupBy(split_by_column) \
            .agg({split_with_column: "count"}) \
            .withColumnRenamed('count(' + split_with_column + ')', "n" + split_with_column) \
            .where(col("n" + split_with_column) >= min_rating) \
            .join(self, split_by_column) \
            .drop("n" + split_with_column)

        return rating_filtered

    @staticmethod
    def stratified_split(self, min_rating, by_customer=True, ratio=0.3, fixed_test_sample=False, sample=3):
        '''
        Perform stratified sampling on rating DataFrame to split into train and test.
        :param min_rating: minimum number of rating for filtering.
        :param ratio: splitting ratio for train and test.
        :param by: by which variable (customer or item) to filter the rating.
        '''
        from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
        from pyspark.sql import SparkSession

        if by_customer:
            by = "customer"
            with_ = "item"
            split_by_column = by + "ID"
            split_with_column = "item" + "ID"
        else:
            by = "item"
            with_ = "customer"
            split_by_column = by + "ID"
            split_with_column = "customer" + "ID"

        rating_joined = self.min_rating_filter(min_rating, by_customer)

        rating_stratified_rdd = rating_joined.groupBy(split_by_column) \
            .agg({split_with_column: "count"}) \
            .withColumnRenamed('count(' + split_with_column + ')', 'n' + with_) \
            .rdd

        perm_indices = rating_stratified_rdd.map(lambda r: (r[0], np.random.permutation(r[1]), r[1]))
        perm_indices.cache()

        if not fixed_test_sample:
            tr_idx = perm_indices.map(lambda r: (r[0], r[1][: int(round(r[2] * ratio))]))
            test_idx = perm_indices.map(lambda r: (r[0], r[1][int(round(r[2] * ratio)):]))
        else:
            tr_idx = perm_indices.map(lambda r: (r[0], r[1][: int(r[2] - sample)]))
            test_idx = perm_indices.map(lambda r: (r[0], r[1][int(r[2] - sample):]))

        if by_customer:
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
            StructField('customerID', IntegerType(), True),
            StructField('itemID', IntegerType(), True),
            StructField('rating', DoubleType(), True)
        ])

        rating_train = SparkSession.builder.getOrCreate().createDataFrame(rating_train, schema)
        rating_test = SparkSession.builder.getOrCreate().createDataFrame(rating_test, schema)

        return rating_train, rating_test

    @staticmethod
    def chronological_split(self, min_rating, by_customer=True, ratio=0.3, fixed_test_sample=False, sample=3):
        '''
        Chronological splitting split data (items are ordered by timestamps for each customer) by timestamps. Fixed ratio and fixed number also apply to splitting.
        :param min_rating: minimum number of rating for filtering.
        :param ratio: sampling ratio for testing set .
        :param fixed_test_sample: whether or not fixing the number in sampling testing data.
        :param sample: number of samples for testing data.
        '''
        from pyspark.sql import Window
        from pyspark.sql.functions import col, bround, row_number

        if by_customer:
            by = "customer"
            with_ = "item"
            split_by_column = by + "ID"
            split_with_column = "item" + "ID"
        else:
            by = "item"
            with_ = "customer"
            split_by_column = by + "ID"
            split_with_column = "customer" + "ID"

        # Check if there is timestamp availabe in the columns.

        rating_grouped = self \
            .groupBy(split_by_column) \
            .agg({'timeStamp': 'count'}) \
            .withColumnRenamed('count(timeStamp)', 'count')

        window_spec = Window.partitionBy(split_by_column).orderBy(col('timeStamp').desc())

        if fixed_test_sample == False:
            rating_all = self.join(rating_grouped, on=split_by_column, how="outer") \
                .withColumn('splitPoint', bround(col('count') * ratio))

            rating_train = rating_all \
                .select('*', row_number().over(window_spec).alias('rank')) \
                .filter(col('rank') > col('splitPoint'))
            rating_test = rating_all \
                .select('*', row_number().over(window_spec).alias('rank')) \
                .filter(col('rank') <= col('splitPoint'))
        else:
            rating_train = self \
                .select('*', row_number().over(window_spec).alias('rank')) \
                .filter(col('rank') > sample)
            rating_test = self \
                .select('*', row_number().over(window_spec).alias('rank')) \
                .filter(col('rank') <= sample)

        rating_train = rating_train.select(split_by_column, split_with_column, "timeStamp")
        rating_test = rating_test.select(split_by_column, split_with_column, "timeStamp")

        return rating_train, rating_test

    @staticmethod
    def non_overlapping_split(self, min_rating, by_customer=True, ratio=0.7, fixed_test_sample=False, sample=3):
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

        rating_joined = self.min_rating_filter(min_rating, by_customer)

        if by_customer:
            by = "customer"
            with_ = "item"
            split_by_column = by + "ID"
            split_with_column = "item" + "ID"
        else:
            by = "item"
            with_ = "customer"
            split_by_column = by + "ID"
            split_with_column = "customer" + "ID"

        rating_exclusive = rating_joined.groupBy(split_by_column) \
            .agg({split_with_column: "count"}) \
            .withColumnRenamed("count(" + split_with_column + ")", "n" + with_) \
            .drop("n" + with_)

        if not fixed_test_sample:
            rating_split = rating_exclusive.randomSplit([ratio, 1 - ratio])
        else:
            count = rating_exclusive.count()

            window_spec = Window.orderBy(rand())
            rating_tmp = rating_exclusive.select(col("*"), row_number().over(window_spec).alias("rowNumber"))

            rating_split = \
                rating_tmp.filter(rating_tmp['rowNumber'] <= (count - sample)).drop("rowNumber"), \
                rating_tmp.filter(rating_tmp['rowNumber'] > (count - sample)).drop('rowNumber')

        rating_train = rating_joined.join(rating_split[0], split_by_column)
        rating_test = rating_joined.join(rating_split[1], split_by_column)

        return rating_train, rating_test

    @staticmethod
    def random_split(self, min_rating, by_customer=True, ratio=0.7, fixed_test_sample=False, sample=3):
        '''
        Purely random splitting. This can be generally used for evaluating either ranking or rating metrics.
        :param min_rating: minimum number of rating for filtering.
        :param ratio: sampling ratio for testing set .
        :param fixed_test_sample: whether or not fixing the number in sampling testing data.
        :param sample: number of samples for testing data.
        '''
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number, col, rand

        if by_customer:
            by = "customer"
            with_ = "item"
            split_by_column = by + "ID"
            split_with_column = "item" + "ID"
        else:
            by = "item"
            with_ = "customer"
            split_by_column = by + "ID"
            split_with_column = "customer" + "ID"

        rating_joined = self.min_rating_filter(min_rating, by_customer)

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


if __name__ == "__main__":
    print("Splitter")
