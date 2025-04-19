---
title: Overview
hide_title: true
sidebar_label: Overview
---

## Causal Inference on Apache Spark

### What is Causal Inference?
One challenge that has taken the spotlight in recent years is using machine learning to drive decision makings in policy and business. 
Often, businesses and policymakers would like to study whether an incentive or intervention will lead to a desired outcome and by how much.
For example, if we give customers a discount (treatment), how much more will they purchase in the future (outcome). 
Traditionally, people use correlation analysis or prediction model to understand correlated factors, but going from prediction to an 
impactful decision isn't always straightforward as correlation doesn't imply causation. In many cases, confounding variables influence 
both the probability of treatment and the outcome, introducing more non-causal correlation. 

Causal inference helps to bridge the gap between prediction and decision-making. 

### Causal Inference language
| Term            | Example                                                            |
|-----------------|--------------------------------------------------------------------|
| Treatment (T)   | Seeing an advertisement                                            |
| Outcome (Y)     | Probability of buying a specific new game                          |
| Confounders (W) | Current gaming habits, past purchases, customer location, platform |

### Causal Inference and Double machine learning
The gold standard approach to isolating causal questions is to run an experiment that randomly assigns the treatment to some customers. 
Randomization eliminates any relationship between the confounders and the probability of treatment,
so any differences between treated and untreated customers can only reflect the direct causal effect of the treatment on the outcome (treatment effect).
However, in many cases, treatments experiments are either impossible or cost prohibitive. 
As a result, we look toward causal inference methods that allow us to estimate the treatment effect using observational data.

The SynapseML causal package implements a technique "Double machine learning", which can be used to estimate the average treatment effect via machine learning models.
Unlike regression-based approaches that make strict parametric assumptions, this machine learning-based approach allows us to model non-linear      relationships between the confounders, treatment, and outcome.

### Usage
In PySpark, you can run the `DoubleMLEstimator` via:

```python
from pyspark.ml.classification import LogisticRegression
from synapse.ml.causal import DoubleMLEstimator
dml = (DoubleMLEstimator()
      .setTreatmentCol("Treatment")
      .setTreatmentModel(LogisticRegression())
      .setOutcomeCol("Outcome")
      .setOutcomeModel(LogisticRegression())
      .setMaxIter(20))
dmlModel = dml.fit(dataset)
```
> Note: all columns except "Treatment" and "Outcome" in your dataset will be used as confounders.

> Note: For discrete treatment, the treatment column must be `int` or `bool`. `0` and `False` will be treated as the control group. 

After fitting the model, you can get average treatment effect and confidence interval:
```python
dmlModel.getAvgTreatmentEffect()
dmlModel.getConfidenceInterval()
```

For an end to end application, check out the DoubleMLEstimator [notebook
example](../Quickstart%20-%20Measure%20Causal%20Effects).
