\# LightGBM - Quantile Regression for Drug Discovery (Scala)



\## Overview \& Background



In pharmaceutical research and drug discovery, predicting the biological activity or potency of chemical compounds (Quantitative Structure-Activity Relationship, or \*\*QSAR\*\*) is a foundational task.



Traditional machine learning regression models optimize for \*\*Mean Squared Error (MSE)\*\*, producing a single point estimate representing the \*average\* activity. However, in drug development, high uncertainty can result in costly laboratory failures.



\*\*Quantile Regression\*\* solves this by estimating conditional percentiles (e.g., 20th percentile, 50th percentile/median, and 80th percentile) of the response variable. This creates an \*\*uncertainty envelope\*\* (confidence interval) around each prediction, allowing medicinal chemists to quantify risk and prioritize stable drug candidates.



\---



\## Key Syntax Differences: PySpark vs. Spark Scala



If you are migrating from the Python SynapseML tutorial, keep these key differences in mind:



| Feature | Python (PySpark) | Scala (Spark) | Explanation |

| :--- | :--- | :--- | :--- |

| \*\*Parameter Configuration\*\* | `LightGBMRegressor(alpha=0.5, objective="quantile")` | `new LightGBMRegressor().setAlpha(0.5).setObjective("quantile")` | Scala uses the \*\*fluent setter pattern\*\* (`.setParam()`) instead of constructor keyword arguments. |

| \*\*Variable Immutability\*\* | `model = ...` | `val model = ...` | Scala uses `val` for immutable variables and `var` for mutable ones. |

| \*\*Array Definitions\*\* | `\[0.7, 0.3]` | `Array(0.7, 0.3)` | Scala requires explicit `Array(...)` collections for methods like `randomSplit`. |

| \*\*Lambdas / Filtering\*\* | `\[c for c in cols if c != "label"]` | `cols.filter(\_ != "label")` | Scala uses concise underscore `\_` syntax for anonymous lambda functions. |

| \*\*Imports\*\* | `import synapse.ml.lightgbm...` | `import com.microsoft.azure.synapse.ml.lightgbm...` | Scala follows full JVM package hierarchy namespaces. |



\---



\## Step 1: Environment Setup and Imports



To use LightGBM in Spark Scala, ensure the `synapseml` Maven package is attached to your Spark cluster or session:

\* \*\*Maven Coordinate:\*\* `com.microsoft.azure:synapseml\_2.12:0.11.4`



Import the required Spark and SynapseML classes:



```scala

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions.\_

import org.apache.spark.ml.feature.VectorAssembler

import org.apache.spark.ml.evaluation.RegressionEvaluator

import com.microsoft.azure.synapse.ml.lightgbm.LightGBMRegressor

