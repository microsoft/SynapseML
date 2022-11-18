---
title: Causal Inference
hide_title: true
sidebar_label: About
---

## Causal Inference on Apache Spark

### What is Causal Inference?
One challenge that machine learning tries to solve is to help decision-makings in policy and business nowadays. 
For example, if we give customers a discount (treatment), how much longer will they stay (outcome). 
Traditionally, people use prediction model to understand correlation, but going from prediction to a decision is not always straightforward, 
correlation is not causation, it's very common that some confounding features influence both the probability of treatment and the outcome, 
creating additional non-causal correlation. 

Causal inference helps to bridge the gap between prediction and decision-making. 

### Causal Inference language
| Term            | Example                                                            |
|-----------------|--------------------------------------------------------------------|
| Treatment (T)   | Seeing an advertisement                                            |
| Outcome (Y)     | Probability of buying a specific new game                          |
| Confounders (W) | Current gaming habits, past purchases, customer location, platform |

### Causal Inference and Double machine learning
The gold standard approach to answering isolating causal questions is to run an experiment that randomly assigns
the treatment to some customers. 
Randomization eliminates any relationship between the confounders and the probability of treatment,
so any differences between treated and untreated customers can only reflect the causal treatment effect.
But it has limitations, some treatments experiments are impossible or cost prohibitive, and some users may not comply with their assignment in experiments.

The SynapseML causal package implements a technique "Double machine learning", which can be used to get treatment effect estimation via machine learning based approaches.

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
dmlModel = dml.fit(df)
dmlModel.getAvgTreatmentEffect()
dmlModel.getConfidenceInterval()
```

For an end to end application, check out the DoubleMLEstimator [notebook
example](../todo).