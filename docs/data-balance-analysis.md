---
title: Data Balance Analysis on Spark
description: Learn how to do Data Balance Analysis on Spark to determine how well features and feature values are represented in your dataset.
---

# Data Balance Analysis on Spark

## Context

Data Balance Analysis is relevant for overall understanding of datasets, but it becomes essential when thinking about building Machine Learning services out of such datasets. Having a well balanced data representation is critical when developing models in a responsible way, specially in terms of fairness. 
It is unfortunately all too easy to build an ML model that produces biased results for subsets of an overall population, by training or testing the model on biased ground truth data. There are multiple case studies of biased models assisting in granting loans, healthcare, recruitment opportunities and many other decision making tasks. In most of these examples, the data from which these models are trained was the common issue. These findings emphasize how important it is for model creators and auditors to analyze data balance: to measure training data across sub-populations and ensure the data has good coverage and a balanced representation of labels across sensitive categories and category combinations, and to check that test data is representative of the target population.

In summary, Data Balance Analysis, used as a step for building ML models has the following benefits:

* <b>Reduces risks for unbalanced models (facilitate service fairness) and reduces costs of ML building</b> by identifying early on data representation gaps that prompt data scientists to seek mitigation steps (collect more data, follow a specific sampling mechanism, create synthetic data, etc.) before proceeding to train their models.
* <b>Enables easy e2e debugging of ML systems </b> in combination with [Fairlearn](https://fairlearn.org/) by providing a clear view if for an unbalanced model the issue is tied to the data or the model.

## Usage

Data Balance Analysis currently supports three transformers in the `com.microsoft.azure.synapse.ml.exploratory` namespace:

* FeatureBalanceMeasure - supervised (requires label column)
* DistributionBalanceMeasure - unsupervised (doesn't require label column)
* AggregateBalanceMeasure - unsupervised (doesn't require label column)

1. Import all three transformers.

    For example:

    ```scala
    import com.microsoft.azure.synapse.ml.exploratory.{AggregateBalanceMeasure, DistributionBalanceMeasure, FeatureBalanceMeasure}
    ```

2. Load your dataset, define features of interest, and ensure that the label column supports binary classification (only contains 0 or 1).

    For example:

    ```scala
    import org.apache.spark.sql.functions.{col, lit}

    val features = Array("race", "sex")
    val label = "income"

    val df = spark.read.parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/AdultCensusIncome.parquet")
        // Convert the "income" column from {<=50K, >50K} to {0, 1} to represent our binary classification label column
        .withColumn(labelCol, when(col(label).contains("<=50K"), lit(0)).otherwise(lit(1)))
    ```

3. Create a `FeatureBalanceMeasure` transformer and use `setSensitiveCols` and `setLabelCol`. Then, call the transformer on your dataset and visualize the resulting dataframe.

    For example:

    ```scala
    val featureBalanceMeasures = new FeatureBalanceMeasure().setSensitiveCols(features).setLabelCol(label).transform(df)
    featureBalanceMeasures.show(truncate = false)
    ```

4. Create a `DistributionBalanceMeasure` transformer and use `setSensitiveCols`. Then, call the transformer on your dataset and visualize the resulting dataframe.

    For example:

    ```scala
    val distributionBalanceMeasures = new DistributionBalanceMeasure().setSensitiveCols(features).transform(df)
    distributionBalanceMeasures.show(truncate = false)
    ```

5. Create a `AggregateBalanceMeasure` transformer and use `setSensitiveCols`. Then, call the transformer on your dataset and visualize the resulting dataframe.

    For example:

    ```scala
    val aggregateBalanceMeasures = new AggregateBalanceMeasure().setSensitiveCols(features).transform(df)
    aggregateBalanceMeasures.show(truncate = false)
    ```

Note: If you are running this notebook in a Spark environment such as Azure Synapse or Databricks, then you can easily visualize the imbalance measures using the built-in plotting features of `display()`.

## Examples

* [Data Balance Analysis - Adult Census Income](../notebooks/Data%20Balance%20Analysis%20-%20Adult%20Census%20Income.ipynb)

## Measure Explanations

### Feature Balance Measures

Feature Balance Measures allow us to see whether each combination of sensitive feature is receiving the positive outcome (true prediction) at equal rates.

In this context, we define a feature balance measure, also referred to as the parity, for label y as the absolute difference between the association metrics of two different sensitive classes \\([x_A, x_B]\\), with respect to the association metric \\(A(x_i, y)\\). That is:

$$parity(y \vert x_A, x_B, A(\cdot)) \coloneqq A(x_A, y) - A(x_B, y) $$

Using the dataset, we can see if the various sexes and races are receiving >50k income at equal or unequal rates.

Note: Many of these metrics were influenced by this paper [Measuring Model Biases in the Absence of Ground Truth](https://arxiv.org/abs/2103.03417).

Measure | Family | Description | Interpretation/Formula | Reference
- | - | - | - | -
Demographic Parity | Fairness | Proportion of each segment of a protected class (e.g. gender) should receive the positive outcome at equal rates. | As close to 0 means better parity. \\(DP = P(Y \vert A = "Male") - P(Y \vert A = "Female")\\). Y = Positive label rate. | [Link](https://en.wikipedia.org/wiki/Fairness_%28machine_learning%29)
Pointwise Mutual Information (PMI), normalized PMI | Entropy | The PMI of a pair of feature values (ex: Gender=Male and Gender=Female) quantifies the discrepancy between the probability of their coincidence given their joint distribution and their individual distributions (assuming independence). | Range (normalized) [-1, 1]. -1 for no co-occurences. 0 for co-occurences at random. 1 for complete co-occurences. | [Link](https://en.wikipedia.org/wiki/Pointwise_mutual_information)
Sorensen-Dice Coefficient (SDC) | Intersection-over-Union | Used to gauge the similarity of two samples. Related to F1 score. | Equals twice the number of elements common to both sets divided by the sum of the number of elements in each set. | [Link](https://en.wikipedia.org/wiki/S%C3%B8rensen%E2%80%93Dice_coefficient)
Jaccard Index | Intersection-over-Union | Similar to SDC, guages the similarity and diversity of sample sets. | Equals the size of the intersection divided by the size of the union of the sample sets. | [Link](https://en.wikipedia.org/wiki/Jaccard_index)
Kendall Rank Correlation | Correlation and Statistical Tests | Used to measure the ordinal association between two measured quantities. | High when observations have a similar rank and low when observations have a dissimilar rank between the two variables. | [Link](https://en.wikipedia.org/wiki/Kendall_rank_correlation_coefficient)
Log-Likelihood Ratio | Correlation and Statistical Tests | Calculates the degree to which data supports one variable versus another. Log of the likelihood ratio, which gives the probability of correctly predicting the label in ratio to probability of incorrectly predicting label. | If likelihoods are similar, it should be close to 0. | [Link](https://en.wikipedia.org/wiki/Likelihood_function#Likelihood_ratio)
t-test | Correlation and Statistical Tests | Used to compare the means of two groups (pairwise). | Value looked up in t-Distribution tell if statistically significant or not. | [Link](https://en.wikipedia.org/wiki/Student's_t-test)

### Distribution Balance Measures

Distribution Balance Measures allow us to compare our data with a reference distribution (i.e. uniform distribution). They are calculated per sensitive column and don't use the label column.

For example, let's assume we have a dataset with 9 rows and a Gender column, and we observe that:
- "Male" appears 4 times
- "Female" appears 3 times
- "Other" appears 2 times

Assuming the uniform distribution:
$$ReferenceCount \coloneqq  \frac{numRows}{numFeatureValues}$$
$$ReferenceProbability \coloneqq  \frac{1}{numFeatureValues}$$

Feature Value | Observed Count | Reference Count | Observed Probability | Reference Probabiliy
- | - | - | - | -
Male | 4 | 9/3 = 3 | 4/9 = 0.44 | 3/9 = 0.33
Female | 3 | 9/3 = 3 | 3/9 = 0.33 | 3/9 = 0.33
Other | 2 | 9/3 = 3 | 2/9 = 0.22 | 3/9 = 0.33

We can use distance measures to find out how far our observed and reference distributions of these feature values are. Some of these distance measures include:

Measure | Description | Interpretation | Reference
- | - | - | -
KL Divergence | Measure of how one probability distribution is different from a second, reference probability distribution. Measure of the information gained when one revises one's beliefs from the prior probability distribution Q to the posterior probability distribution P. In other words, it is the amount of information lost when Q is used to approximate P. | Non-negative. 0 means P = Q. | [Link](https://en.wikipedia.org/wiki/Kullback%E2%80%93Leibler_divergence)
JS Distance | Measuring the similarity between two probability distributions. Symmetrized and smoothed version of the Kullback–Leibler (KL) divergence. Square root of JS Divergence. | Range [0, 1]. 0 means perfectly same to balanced distribution. | [Link](https://en.wikipedia.org/wiki/Jensen%E2%80%93Shannon_divergence)
Wasserstein Distance | This distance is also known as the earth mover’s distance, since it can be seen as the minimum amount of “work” required to transform u into v, where “work” is measured as the amount of distribution weight that must be moved, multiplied by the distance it has to be moved. | Non-negative. 0 means P = Q. | [Link](https://en.wikipedia.org/wiki/Wasserstein_metric)
Infinity Norm Distance | Distance between two vectors is the greatest of their differences along any coordinate dimension. Also called Chebyshev distance or chessboard distance. | Non-negative. 0 means same distribution. | [Link](https://en.wikipedia.org/wiki/Chebyshev_distance)
Total Variation Distance | It is equal to half the L1 (Manhattan) distance between the two distributions. Take the difference between the two proportions in each category, add up the absolute values of all the differences, and then divide the sum by 2. | Non-negative. 0 means same distribution. | [Link](https://en.wikipedia.org/wiki/Total_variation_distance_of_probability_measures)
Chi-Squared Test | The chi-square test tests the null hypothesis that the categorical data has the given frequencies given expected frequencies in each category. | p-value gives evidence against null-hypothesis that difference in observed and expected frequencies is by random chance. | [Link](https://en.wikipedia.org/wiki/Chi-squared_test)

### Aggregate Balance Measures

Aggregate Balance Measures allow us to obtain a higher notion of inequality. They are calculated on the global set of sensitive columns and don't use the label column.

These measures look at distribution of records across all combinations of sensitive columns. For example, if Sex and Race are sensitive columns, it shall try to quantify imbalance across all combinations - (Male, Black), (Female, White), (Male, Asian-Pac-Islander), etc.

Measure | Description | Interpretation | Reference
- | - | - | -
Atkinson Index | It presents the percentage of total income that a given society would have to forego in order to have more equal shares of income between its citizens. This measure depends on the degree of society aversion to inequality (a theoretical parameter decided by the researcher), where a higher value entails greater social utility or willingness by individuals to accept smaller incomes in exchange for a more equal distribution. An important feature of the Atkinson index is that it can be decomposed into within-group and between-group inequality. | Range [0, 1]. 0 if perfect equality. 1 means maximum inequality. In our case, it is the proportion of records for a sensitive columns’ combination. | [Link](https://en.wikipedia.org/wiki/Atkinson_index)
Theil T Index | GE(1) = Theil's T and is more sensitive to differences at the top of the distribution. The Theil index is a statistic used to measure economic inequality. The Theil index measures an entropic "distance" the population is away from the "ideal" egalitarian state of everyone having the same income. | If everyone has the same income, then T_T equals 0. If one person has all the income, then T_T gives the result (ln N). 0 means equal income and larger values mean higher level of disproportion. | [Link](https://en.wikipedia.org/wiki/Theil_index)
Theil L Index | GE(0) = Theil's L and is more sensitive to differences at the lower end of the distribution. Logarithm of (mean income)/(income i), over all the incomes included in the summation. It is also referred to as the mean log deviation measure. Because a transfer from a larger income to a smaller one will change the smaller income's ratio more than it changes the larger income's ratio, the transfer-principle is satisfied by this index. | Same interpretation as Theil T Index. | [Link](https://en.wikipedia.org/wiki/Theil_index)
