---
title: Quickstart - Measure Causal Effects
hide_title: true
status: stable
---
# Startup Investment Attribution - Understand Outreach Effort's Effect"

**This sample notebook aims to show the application of using SynapseML's DoubleMLEstimator for inferring causality using observational data.**

A startup that sells software would like to know whether its outreach efforts were successful in attracting new customers or boosting consumption among existing customers. In other words, they would like to learn the treatment effect of each investment on customers' software usage.

In an ideal world, the startup would run several randomized experiments where each customer would receive a random assortment of investments. However, this can be logistically prohibitive or strategically unsound: the startup might not have the resources to design such experiments or they might not want to risk losing out on big opportunities due to lack of incentives.

In this customer scenario walkthrough, we show how SynapseML causal package can use historical investment data to learn the investment effect.

## Background
In this scenario, a startup that sells software provides discounts incentives to its customer. A customer might be given or not.

The startup has historical data on these investments for 2,000 customers, as well as how much revenue these customers generated in the year after the investments were made. They would like to use this data to learn the optimal incentive policy for each existing or new customer in order to maximize the return on investment (ROI).

The startup faces a challenge:  the dataset is biased because historically the larger customers received the most incentives. Thus, they need a causal model that can remove the bias.

## Data
The data* contains ~2,000 customers and is comprised of:

* Customer features: details about the industry, size, revenue, and technology profile of each customer.
* Interventions: information about which incentive was given to a customer.
* Outcome: the amount of product the customer bought in the year after the incentives were given.


| Feature Name    | Type | Details                                                                                                                                     |
|-----------------|------|---------------------------------------------------------------------------------------------------------------------------------------------|
| Global Flag     | W    | whether the customer has global offices                                                                                                     | 
| Major Flag      | W    | whether the customer is a large consumer in their industry (as opposed to SMC - Small Medium Corporation - or SMB - Small Medium Business)  |
| SMC Flag        | W    | whether the customer is a Small Medium Corporation (SMC, as opposed to major and SMB)                                                       |
| Commercial Flag | W    | whether the customer's business is commercial (as opposed to public secor)                                                                  |
| IT Spend        | W    | $ spent on IT-related purchases                                                                                                             |
| Employee Count  | W    | number of employees                                                                                                                         |
| PC Count        | W    | number of PCs used by the customer                                                                                                          |                                                                                      |
| Discount        | T    | whether the customer was given a discount (binary)                                                                                          |
| Revenue         | Y    | $ Revenue from customer given by the amount of software purchased                                                                           |



```python
# Import the sample multi-attribution data
data = (
    spark.read.format("csv")
    .option("inferSchema", True)
    .option("header", True)
    .load(
        "wasbs://publicwasb@mmlspark.blob.core.windows.net/multi_attribution_sample.csv"
    )
)
```

# Get Causal Effects with SynapseML DoubleMLEstimator


```python
from synapse.ml.causal import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.regression import LinearRegression

treatmentColumn = "Discount"
outcomeColumn = "Revenue"

dml = (
    DoubleMLEstimator()
    .setTreatmentModel(LogisticRegression())
    .setTreatmentCol(treatmentColumn)
    .setOutcomeModel(LinearRegression())
    .setOutcomeCol(outcomeColumn)
    .setMaxIter(20)
)

model = dml.fit(data)
```


```python
# Get average treatment effect, it returns a numeric value, e.g. 5166.78324
# It means, on average, customers who received a discount spent $5,166 more on software
model.getAvgTreatmentEffect()
```


```python
# Get treatment effect's confidence interval, e.g.  [4765.826181160708, 5371.2817538168965]
model.getConfidenceInterval()
```
