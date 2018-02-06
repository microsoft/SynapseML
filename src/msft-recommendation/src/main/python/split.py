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
import pandas as pd

# Methods for train-test split.

def min_rating_filter(self, min_rating, by_customer):
    '''
    Filter rating DataFrame for each user with minimum rating.
    :param min_rating: minimum number of rating for filtering.
    :param by: by which variable (customer or item) to filter the rating.
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

    return(rating_filtered)

def stratified_split(self, min_rating, by_customer=True, ratio=0.3, fixed_test_sample=False, sample=3):
    '''
    Perform stratified sampling on rating DataFrame to split into train and test.
    :param min_rating: minimum number of rating for filtering.
    :param ratio: splitting ratio for train and test.
    :param by: by which variable (customer or item) to filter the rating.
    '''
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
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
        tr_by = rating_joined.rdd.groupBy(lambda r: r[0]).join(tr_idx).flatMap( lambda r: np.array([x for x in r[1][0]]) [r[1][1]] )
        test_by = rating_joined.rdd.groupBy(lambda r: r[0]).join(test_idx).flatMap( lambda r: np.array([x for x in r[1][0]]) [r[1][1]] )
    else:
        tr_by = rating_joined.rdd.groupBy(lambda r: r[1]).join(tr_idx).flatMap( lambda r: np.array([x for x in r[1][0]]) [r[1][1]] )
        test_by = rating_joined.rdd.groupBy(lambda r: r[1]).join(test_idx).flatMap( lambda r: np.array([x for x in r[1][0]]) [r[1][1]] )

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

    rating_joined = self.min_rating_filter(min_rating, by_customer) 

    rating_grouped = rating_joined \
        .groupBy(split_by_column) \
        .agg({'timeStamp': 'count'}) \
        .withColumnRenamed('count(timeStamp)', 'count') 

    window_spec = Window.partitionBy(split_by_column).orderBy(col('timeStamp').desc())

    if fixed_test_sample == False:
        rating_all = rating_joined.join(rating_grouped, on = split_by_column, how="outer") \
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

    rating_train = rating_train.select(split_by_column, split_with_column, "timeStamp")
    rating_test = rating_test.select(split_by_column, split_with_column, "timeStamp")
    
    return rating_train, rating_test

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
