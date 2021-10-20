---
title: Datasets
hide_title: true
sidebar_label: Datasets
---

# Datasets Used in Sample Jupyter Notebooks

## Adult Census Income

A small dataset that can be used to predict income level of a person given
demographic features.  The dataset is a comma-separated file with 15 columns and
32561 rows.  The columns have numeric and categorical string types.

The example dataset is available
[here](https://mmlspark.azureedge.net/datasets/AdultCensusIncome.csv); the
original dataset is from [the UCI Machine Learning
Repository](https://archive.ics.uci.edu/ml/datasets/Adult).  The example dataset
has been cleaned of rows with missing values.

Reference: _Kohavi, R., Becker, B., (1996)_, \\
    [UCI Machine Learning Repository](http://archive.ics.uci.edu/ml), Irvine, CA, \\
    University of California, School of Information and Computer Science.

## Book Reviews from Amazon

This dataset can be used to predict sentiment of book reviews.  The dataset is a
tab-separated file with 2 columns (`rating`, `text`) and 10000 rows.  The
`rating` column has integer values of 1, 2, 4 or 5, and the `text` column
contains free-form text strings in English language.  You can use
`synapse.ml.TextFeaturizer` to convert the text into feature vectors for machine
learning models ([see
example](../../examples/text_analytics/TextAnalytics%20-%20Amazon%20Book%20Reviews/)).

The example dataset is available
[here](https://mmlspark.azureedge.net/datasets/BookReviewsFromAmazon10K.tsv);
the original dataset is available from [Dredze's
page](http://www.cs.jhu.edu/~mdredze/datasets/sentiment/).  The example dataset
has been sampled down to 10000 rows.

Reference: _Biographies, Bollywood, Boom-boxes and Blenders: Domain Adaptation
for Sentiment Classification_, \\
    John Blitzer, Mark Dredze, and Fernando Pereira \\
    Association of Computational Linguistics (ACL), 2007.

## CIFAR-10 Images

A collection of small labeled images.  This dataset can be used to train and
evaluate deep neural network models for image classification.  The dataset
consists of 60000 32-by-32 RGB images, labeled into 10 categories.

The example dataset is available
[here](https://mmlspark.azureedge.net/datasets/CIFAR10/cifar-10-python.tar.gz);
the original dataset is available [Krizhevsky's
page](https://www.cs.toronto.edu/~kriz/cifar.html).  The dataset has been
packaged into a gzipped tar archive.  See notebook [301 - CIFAR10 CNTK CNN
Evaluation](../notebooks/301%20-%20CIFAR10%20CNTK%20CNN%20Evaluation.ipynb)
for an example how to extract the image data.

Reference: [_Learning Multiple Layers of Features from Tiny
Images_](https://www.cs.toronto.edu/~kriz/learning-features-2009-TR.pdf), \\
    Alex Krizhevsky, Mater thesis, 2009.

## Flight On-Time Performance

A medium-small dataset of U.S. passenger flight records from September 2012 that
can be used to predict flight arrival delays.  The dataset is a comma-separated
file with 13 columns that have numeric and categorical string types.  The
dataset has 490199 rows total, and 485430 rows if records with missing values —
mostly diverted flights — are excluded.

The example dataset is available
[here](https://mmlspark.azureedge.net/datasets/On_Time_Performance_2012_9.csv);
the original data is available from [the TranStats web
site](http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time).
The example dataset contains a subset of columns from original data, to trim the
data size.

Reference: _TranStats data collection_, \\
    Bureau of Transportation Statistics, \\
    U.S. Department of Transportation.
