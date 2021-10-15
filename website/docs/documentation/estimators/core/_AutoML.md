import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import DocTable from "@theme/DocumentationTable";

<!-- 
```python
import pyspark
import os
import json
from IPython.display import display

spark = (pyspark.sql.SparkSession.builder.appName("MyApp")
        .config("spark.jars.packages", "com.microsoft.azure:synapseml:0.9.0")
        .config("spark.jars.repositories", "https://mmlspark.azureedge.net/maven")
        .getOrCreate())

def getSecret(secretName):
        get_secret_cmd = 'az keyvault secret show --vault-name mmlspark-build-keys --name {}'.format(secretName)
        value = json.loads(os.popen(get_secret_cmd).read())["value"]
        return value

import synapse.ml
``` 
-->

## FindBestModel

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
display(bestModel.transform(df))
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
display(bestModel.transform(df))
```

</TabItem>
</Tabs>

<DocTable className="FindBestModel"
py="mmlspark.automl.html#module-mmlspark.automl.FindBestModel"
scala="com/microsoft/ml/spark/automl/FindBestModel.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/automl/FindBestModel.scala" />


## TuneHyperparameters

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
display(tuneHyperparameters.fit(dataset))
```

</TabItem>
</Tabs>

<DocTable className="TuneHyperparameters"
py="mmlspark.automl.html#module-mmlspark.automl.TuneHyperparameters"
scala="com/microsoft/ml/spark/automl/TuneHyperparameters.html"
sourceLink="https://github.com/microsoft/SynapseML/blob/master/core/src/main/scala/com/microsoft/azure/synapse/ml/automl/TuneHyperparameters.scala" />

