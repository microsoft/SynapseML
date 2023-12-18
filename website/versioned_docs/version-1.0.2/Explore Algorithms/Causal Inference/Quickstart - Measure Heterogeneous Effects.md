---
title: Quickstart - Measure Heterogeneous Effects
hide_title: true
status: stable
---
# Startup Investment Attribution - Understand Outreach Effort's Effect"

![image-alt-text](https://camo.githubusercontent.com/4ac8c931fd4600d2b466975c87fb03b439ebc7f6debd58409aea0db10457436d/68747470733a2f2f7777772e6d6963726f736f66742e636f6d2f656e2d75732f72657365617263682f75706c6f6164732f70726f642f323032302f30352f4174747269627574696f6e2e706e67)

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
| IT Spend        | W    | dollar spent on IT-related purchases                                                                                                             |
| Employee Count  | W    | number of employees                                                                                                                         |
| PC Count        | W    | number of PCs used by the customer                                                                                                          |                                                                                      |
| Size            | X    | customer's size given by their yearly total revenue                                                                                        |                                                                                      |
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

# Get Heterogenous Causal Effects with SynapseML OrthoDML Estimator


```python
data.columns
```


```python
from synapse.ml.causal import *
from pyspark.ml import Pipeline
from synapse.ml.causal import *
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import IntegerType, BooleanType, DateType, DoubleType
import matplotlib.pyplot as plt
```


```python
treatmentColumn = "Discount"
outcomeColumn = "Revenue"
confounderColumns = [
    "Global Flag",
    "Major Flag",
    "SMC Flag",
    "Commercial Flag",
    "Employee Count",
    "PC Count",
]
heteroColumns = ["Size", "IT Spend"]
heterogeneityVecCol = "XVec"
confounderVecCol = "XWVec"

data = data.withColumn(treatmentColumn, data.Discount.cast(DoubleType()))

heterogeneityVector = VectorAssembler(
    inputCols=heteroColumns, outputCol=heterogeneityVecCol
)

confounderVector = VectorAssembler(
    inputCols=confounderColumns, outputCol=confounderVecCol
)

pipeline = Pipeline(stages=[heterogeneityVector, confounderVector])

ppfit = pipeline.fit(data).transform(data)
```


```python
### Create the Ortho Forest DML Estimator Model
mtTransform = (
    OrthoForestDMLEstimator()
    .setNumTrees(100)
    .setTreatmentCol(treatmentColumn)
    .setOutcomeCol(outcomeColumn)
    .setHeterogeneityVecCol(heterogeneityVecCol)
    .setConfounderVecCol(confounderVecCol)
    .setMaxDepth(10)
    .setMinSamplesLeaf(10)
)
```


```python
### Fit the model for the data
finalModel = mtTransform.fit(ppfit)
```


```python
### Transform the input data to see the model in action
finalPred = finalModel.transform(ppfit)
```


```python
### Get the data in Pandas
pd_final = finalPred.toPandas()
```


```python
### Plot and see the non-linear effects
plt.scatter("Size", mtTransform.getOutputCol(), data=pd_final)
```
