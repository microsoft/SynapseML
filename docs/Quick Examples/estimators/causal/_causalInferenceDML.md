import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";


## DoubleMLEstimator

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.causal import *
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, BooleanType

schema = StructType([
    StructField("Treatment", BooleanType()),
    StructField("Outcome", BooleanType()),
    StructField("col2", DoubleType()),
    StructField("col3", DoubleType()),
    StructField("col4", DoubleType())
    ])


df = spark.createDataFrame([
      (False, True, 0.30, 0.66, 0.2),
      (True, False, 0.38, 0.53, 1.5),
      (False, True, 0.68, 0.98, 3.2),
      (True, False, 0.15, 0.32, 6.6),
      (False, True, 0.50, 0.65, 2.8),
      (True, True, 0.40, 0.54, 3.7),
      (False, True, 0.78, 0.97, 8.1),
      (True, False, 0.12, 0.32, 10.2),
      (False, True, 0.35, 0.63, 1.8),
      (True, False, 0.45, 0.57, 4.3),
      (False, True, 0.75, 0.97, 7.2),
      (True, True, 0.16, 0.32, 11.7)], schema
)

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

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.causal._
import org.apache.spark.ml.classification.LogisticRegression

val df = (Seq(
  (false, true, 0.50, 0.60, 0),
  (true, false, 0.40, 0.50, 1),
  (false, true, 0.78, 0.99, 2),
  (true, false, 0.12, 0.34, 3),
  (false, true, 0.50, 0.60, 0),
  (true, false, 0.40, 0.50, 1),
  (false, true, 0.78, 0.99, 2),
  (true, false, 0.12, 0.34, 3),
  (false, false, 0.50, 0.60, 0),
  (true, true, 0.40, 0.50, 1),
  (false, true, 0.78, 0.99, 2),
  (true, false, 0.12, 0.34, 3))
  .toDF("Treatment", "Outcome", "col2", "col3", "col4"))

val dml = (new DoubleMLEstimator()
  .setTreatmentCol("Treatment")
  .setTreatmentModel(new LogisticRegression())
  .setOutcomeCol("Outcome")
  .setOutcomeModel(new LogisticRegression())
  .setMaxIter(20))

val dmlModel = dml.fit(df)
dmlModel.getAvgTreatmentEffect
dmlModel.getConfidenceInterval
```

</TabItem>
</Tabs>

<DocTable className="DoubleMLEstimator"
py="synapse.ml.causal.html#module-synapse.ml.causal.DoubleMLEstimator"
scala="com/microsoft/azure/synapse/ml/causal/DoubleMLEstimator.html"
csharp="classSynapse_1_1ML_1_1Causal_1_1DoubleMLEstimator.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/causal/DoubleMLEstimator.scala" />

## Multiple continuous treatments

The following **synthetic, unitless** example estimates the joint coefficients
of T1 and T2 (population values 2 and -1.5). It does not reinterpret a binary
treatment as continuous. Both nuisance models are explicit regressors; neither
the outcome nor any treatment is included among the nuisance features.
The RNG seed fixes synthetic data only; Spark splits and bootstraps remain
unseeded and fitted values can vary.

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
import numpy as np
from pyspark.ml.regression import LinearRegression
from synapse.ml.causal import DoubleMLEstimator

rng = np.random.default_rng(2328)
x, z, u, v, error = rng.normal(size=(5, 400))
t1 = 0.8 * x + u
t2 = -0.4 * x + 0.3 * z + v
y = 2.0 * t1 - 1.5 * t2 + 0.7 * x - 0.5 * z + error
plural_df = spark.createDataFrame(
    [(float(a), float(b), float(c), float(d), float(e))
     for a, b, c, d, e in zip(x, z, t1, t2, y)],
    ["x", "z", "T1", "T2", "Y"],
)
plural = (
    DoubleMLEstimator()
    .setTreatmentCols(["T1", "T2"])
    .setTreatmentModel(LinearRegression(regParam=0.0))
    .setOutcomeCol("Y")
    .setOutcomeModel(LinearRegression(regParam=0.0))
    .setMaxIter(30)
)
plural_model = plural.fit(plural_df)
list(zip(
    plural_model.getFittedTreatmentCols(),
    plural_model.getAvgTreatmentEffects(),
    plural_model.getConfidenceIntervals(),
    plural_model.getPValues(),
))
plural_model.getRawTreatmentEffectRows()  # one ordered coefficient vector per draw
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.causal._
import org.apache.spark.ml.regression.LinearRegression

val random = new scala.util.Random(2328)
val pluralDF = (1 to 400).map { _ =>
  val x = random.nextGaussian()
  val z = random.nextGaussian()
  val t1 = 0.8 * x + random.nextGaussian()
  val t2 = -0.4 * x + 0.3 * z + random.nextGaussian()
  val y = 2.0 * t1 - 1.5 * t2 + 0.7 * x - 0.5 * z + random.nextGaussian()
  (x, z, t1, t2, y)
}.toDF("x", "z", "T1", "T2", "Y")
val plural = new DoubleMLEstimator()
  .setTreatmentCols(Array("T1", "T2"))
  .setTreatmentModel(new LinearRegression().setRegParam(0.0))
  .setOutcomeCol("Y")
  .setOutcomeModel(new LinearRegression().setRegParam(0.0))
  .setMaxIter(30)
val pluralModel = plural.fit(pluralDF)
pluralModel.getFittedTreatmentCols
pluralModel.getAvgTreatmentEffects
pluralModel.getConfidenceIntervals
pluralModel.getPValues
pluralModel.getRawTreatmentEffectRows
```

</TabItem>
</Tabs>

Point estimates are bootstrap means. Each plural draw splits original rows
before bootstrapping within each fold, fits a Gaussian GLR with an intercept in
each fold, then averages the fold vectors. This differs from EconML's pooled,
no-intercept final stage. The numerical residual-variance/eigenvalue guards do
not establish causal identification, and extreme scales can still overflow or
underflow. A failed plural draw fails the fit; already-running draws need not
be cancelled. Plural-owned caches are cleaned up, but existing shared trainers
may retain their own failure-path caches.

Intervals are coordinate-wise bootstrap percentiles; p-values are approximate
normal-bootstrap Wald tests using the sample SD of the draws, **not SD divided
by square root of B**. At least two draws are needed. Zero or nonfinite SD gives
`NaN` for that coordinate. There is no multiplicity correction, joint coverage,
or EconML inference equivalence; B = 30 gives rough standard errors and coarse
tails. Choose a defensible causal adjustment set before interpreting real data.

Use only the matching singular or plural getter family. Plural JVM getters
check persisted fitted order and width against current JVM treatment names.
These snapshots are mutable Spark params. Python-only setters are not
transferred by convenience getters; those getters report JVM fitted state.
Generated raw getters may raise `KeyError` for unset/wrong-route parameters.
Do not set both treatment APIs or use `weightCol` with plural treatments.

The singular examples above are unchanged, including `maxIter = 20`. Their
legacy bootstrap resamples before splitting, allowing original-row copies to
cross folds. Their p-value remains the existing t-test of the draw mean and
therefore differs from plural inference. Neither legacy behavior nor shared
trainer failure-path cache handling is repaired by this addition.
