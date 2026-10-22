import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";


## AutoML

### FindBestModel

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">

<!--pytest-codeblocks:cont-->

```python
from synapse.ml.automl import *
from synapse.ml.train import *
from pyspark.ml.classification import RandomForestClassifier

df = (spark.createDataFrame([
    (0, 2, 0.50, 0.60, 0),
    (1, 3, 0.40, 0.50, 1),
    (0, 4, 0.78, 0.99, 2),
    (1, 5, 0.12, 0.34, 3),
    (0, 1, 0.50, 0.60, 0),
    (1, 3, 0.40, 0.50, 1),
    (0, 3, 0.78, 0.99, 2),
    (1, 4, 0.12, 0.34, 3),
    (0, 0, 0.50, 0.60, 0),
    (1, 2, 0.40, 0.50, 1),
    (0, 3, 0.78, 0.99, 2),
    (1, 4, 0.12, 0.34, 3)
], ["Label", "col1", "col2", "col3", "col4"]))

# mocking models
randomForestClassifier = (TrainClassifier()
      .setModel(RandomForestClassifier()
        .setMaxBins(32)
        .setMaxDepth(5)
        .setMinInfoGain(0.0)
        .setMinInstancesPerNode(1)
        .setNumTrees(20)
        .setSubsamplingRate(1.0)
        .setSeed(0))
      .setFeaturesCol("mlfeatures")
      .setLabelCol("Label"))
model = randomForestClassifier.fit(df)

findBestModel = (FindBestModel()
  .setModels([model, model])
  .setEvaluationMetric("accuracy"))
bestModel = findBestModel.fit(df)
bestModel.transform(df).show()
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.automl._
import com.microsoft.azure.synapse.ml.train._
import spark.implicits._
import org.apache.spark.ml.Transformer

val df = (Seq(
      (0, 2, 0.50, 0.60, 0),
      (1, 3, 0.40, 0.50, 1),
      (0, 4, 0.78, 0.99, 2),
      (1, 5, 0.12, 0.34, 3),
      (0, 1, 0.50, 0.60, 0),
      (1, 3, 0.40, 0.50, 1),
      (0, 3, 0.78, 0.99, 2),
      (1, 4, 0.12, 0.34, 3),
      (0, 0, 0.50, 0.60, 0),
      (1, 2, 0.40, 0.50, 1),
      (0, 3, 0.78, 0.99, 2),
      (1, 4, 0.12, 0.34, 3)
  ).toDF("Label", "col1", "col2", "col3", "col4"))

// mocking models
val randomForestClassifier = (new TrainClassifier()
      .setModel(
        new RandomForestClassifier()
        .setMaxBins(32)
        .setMaxDepth(5)
        .setMinInfoGain(0.0)
        .setMinInstancesPerNode(1)
        .setNumTrees(20)
        .setSubsamplingRate(1.0)
        .setSeed(0L))
      .setFeaturesCol("mlfeatures")
      .setLabelCol("Label"))
val model = randomForestClassifier.fit(df)

val findBestModel = (new FindBestModel()
  .setModels(Array(model.asInstanceOf[Transformer], model.asInstanceOf[Transformer]))
  .setEvaluationMetric("accuracy"))
val bestModel = findBestModel.fit(df)
bestModel.transform(df).show()
```

</TabItem>
</Tabs>

<DocTable className="FindBestModel"
py="synapse.ml.automl.html#module-synapse.ml.automl.FindBestModel"
scala="com/microsoft/azure/synapse/ml/automl/FindBestModel.html"
csharp="classSynapse_1_1ML_1_1Automl_1_1FindBestModel.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/automl/FindBestModel.scala" />


### TuneHyperparameters

<Tabs
defaultValue="py"
values={[
{label: `Python`, value: `py`},
{label: `Scala`, value: `scala`},
]}>
<TabItem value="py">


<!--pytest-codeblocks:cont-->

```python
from synapse.ml.automl import *
from synapse.ml.train import *
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier


df = (spark.createDataFrame([
    (0, 1, 1, 1, 1, 1, 1.0, 3, 1, 1),
    (0, 1, 1, 1, 1, 2, 1.0, 1, 1, 1),
    (0, 1, 1, 1, 1, 2, 1.0, 2, 1, 1),
    (0, 1, 2, 3, 1, 2, 1.0, 3, 1, 1),
    (0, 3, 1, 1, 1, 2, 1.0, 3, 1, 1)
], ["Label", "Clump_Thickness", "Uniformity_of_Cell_Size",
    "Uniformity_of_Cell_Shape", "Marginal_Adhesion", "Single_Epithelial_Cell_Size",
    "Bare_Nuclei", "Bland_Chromatin", "Normal_Nucleoli", "Mitoses"]))

logReg = LogisticRegression()
randForest = RandomForestClassifier()
gbt = GBTClassifier()
smlmodels = [logReg, randForest, gbt]
mmlmodels = [TrainClassifier(model=model, labelCol="Label") for model in smlmodels]

paramBuilder = (HyperparamBuilder()
    .addHyperparam(logReg, logReg.regParam, RangeHyperParam(0.1, 0.3))
    .addHyperparam(randForest, randForest.numTrees, DiscreteHyperParam([5,10]))
    .addHyperparam(randForest, randForest.maxDepth, DiscreteHyperParam([3,5]))
    .addHyperparam(gbt, gbt.maxBins, RangeHyperParam(8,16))
    .addHyperparam(gbt, gbt.maxDepth, DiscreteHyperParam([3,5])))
searchSpace = paramBuilder.build()
# The search space is a list of params to tuples of estimator and hyperparam
randomSpace = RandomSpace(searchSpace)

bestModel = TuneHyperparameters(
              evaluationMetric="accuracy", models=mmlmodels, numFolds=2,
              numRuns=len(mmlmodels) * 2, parallelism=2,
              paramSpace=randomSpace.space(), seed=0).fit(df)
```

</TabItem>
<TabItem value="scala">

```scala
import com.microsoft.azure.synapse.ml.automl._
import com.microsoft.azure.synapse.ml.train._
import spark.implicits._

val logReg = new LogisticRegression()
val randForest = new RandomForestClassifier()
val gbt = new GBTClassifier()
val smlmodels = Seq(logReg, randForest, gbt)
val mmlmodels = smlmodels.map(model => new TrainClassifier().setModel(model).setLabelCol("Label"))

val paramBuilder = new HyperparamBuilder()
  .addHyperparam(logReg.regParam, new DoubleRangeHyperParam(0.1, 0.3))
  .addHyperparam(randForest.numTrees, new DiscreteHyperParam(List(5,10)))
  .addHyperparam(randForest.maxDepth, new DiscreteHyperParam(List(3,5)))
  .addHyperparam(gbt.maxBins, new IntRangeHyperParam(8,16))
.addHyperparam(gbt.maxDepth, new DiscreteHyperParam(List(3,5)))
val searchSpace = paramBuilder.build()
val randomSpace = new RandomSpace(searchSpace)

val dataset: DataFrame = Seq(
  (0, 1, 1, 1, 1, 1, 1.0, 3, 1, 1),
  (0, 1, 1, 1, 1, 2, 1.0, 1, 1, 1),
  (0, 1, 1, 1, 1, 2, 1.0, 2, 1, 1),
  (0, 1, 2, 3, 1, 2, 1.0, 3, 1, 1),
  (0, 3, 1, 1, 1, 2, 1.0, 3, 1, 1))
  .toDF("Label", "Clump_Thickness", "Uniformity_of_Cell_Size",
    "Uniformity_of_Cell_Shape", "Marginal_Adhesion", "Single_Epithelial_Cell_Size",
    "Bare_Nuclei", "Bland_Chromatin", "Normal_Nucleoli", "Mitoses")

val tuneHyperparameters = new TuneHyperparameters().setEvaluationMetric("accuracy")
  .setModels(mmlmodels.toArray).setNumFolds(2).setNumRuns(mmlmodels.length * 2)
  .setParallelism(1).setParamSpace(randomSpace).setSeed(0)
tuneHyperparameters.fit(dataset).show()
```

</TabItem>
</Tabs>

#### Learning-to-rank tuning

`TuneHyperparameters` also accepts rankers with `evaluationMetric="ndcg"` or
`"ndcg@k"` (positive integer `k`). Whole query groups are assigned to folds;
at least `numFolds` distinct, non-null string, integer or long groups are required.

<Tabs groupId="code">
<TabItem value="py" label="Python">

<!--pytest-codeblocks:cont-->

```python
from pyspark.ml.linalg import Vectors
from synapse.ml.automl import TuneHyperparameters
from synapse.ml.automl.HyperparamBuilder import HyperparamBuilder, DiscreteHyperParam, RandomSpace
from synapse.ml.lightgbm import LightGBMRanker

ranking_data = spark.createDataFrame(
    [(f"q{q}", float(label), Vectors.dense(float(label), float(q)))
     for q in range(8) for label in range(3)],
    ["query", "label", "features"],
)
ranker = LightGBMRanker(groupCol="query", numIterations=5, minDataInLeaf=1)
search = HyperparamBuilder().addHyperparam(
    ranker, ranker.numLeaves, DiscreteHyperParam([2, 3])
).build()
best = TuneHyperparameters(
    models=[ranker], paramSpace=RandomSpace(search).space(), evaluationMetric="ndcg@5",
    numFolds=2, numRuns=2, parallelism=1, seed=0,
).fit(ranking_data)
best.transform(ranking_data).show()
```

</TabItem>
</Tabs>

The metric averages query-level graded NDCG within each validation fold, then
averages the fold scores. Labels must be integers from 0 through 30; gain is
`2^label - 1` and discount is `1/log2(rank + 1)`. Prediction ties use label
ascending (pessimistic), and all-zero queries score 1. This matches LightGBM's
default gain and discount, but does not promise identical native evaluation
output: tie ordering, query weighting, `evalAt` and `maxPosition` are not mirrored.
Custom `labelGain` and `label_gain` via `passThroughArgs` are rejected.
Previously fitted reference datasets are cleared on each fresh estimator copy.
Group, label and prediction columns must agree across all candidates and runs;
the search space cannot vary group, label, features, prediction or gain settings.

Distinct query groups are sorted by a seeded hash and dealt round-robin into
folds. This keeps groups intact and folds non-empty and balanced, but the
assignment window runs in one partition over the distinct query groups. Plan
for that group-cardinality limit. Classification and regression tuning keep
their existing behavior. `getBestModelInfo()` remains unsupported for rankers.
This example does not cover additional data formats or external tuning libraries.

<DocTable className="TuneHyperparameters"
py="synapse.ml.automl.html#module-synapse.ml.automl.TuneHyperparameters"
scala="com/microsoft/azure/synapse/ml/automl/TuneHyperparameters.html"
csharp="classSynapse_1_1ML_1_1Automl_1_1TuneHyperparameters.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/automl/TuneHyperparameters.scala" />
