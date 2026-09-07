# LightGBM - Quantile Regression for Drug Discovery (Scala)

## Overview & Background

In pharmaceutical research and drug discovery, predicting the biological activity or potency of chemical compounds (Quantitative Structure-Activity Relationship, or **QSAR**) is a foundational task.

Traditional machine learning regression models optimize for **Mean Squared Error (MSE)**, producing a single point estimate representing the *conditional mean* activity. However, in lead optimization and drug candidate selection, point estimates alone can be misleading:
* Experimental assays have intrinsic measurement noise.
* Novel chemical scaffolds often reside in sparse regions of chemical space (out-of-domain), where model confidence is naturally lower.
* High variance and uncertainty can lead to costly laboratory synthesis and in vitro assay failures.

**Quantile Regression** addresses this challenge by estimating conditional percentiles (e.g., 20th percentile, 50th percentile / median, and 80th percentile) of the response distribution. Fitting models across multiple quantiles produces an **uncertainty envelope** (prediction interval) for every candidate compound. This empowers medicinal chemists to quantify risk, prioritize high-confidence candidates, and flag compounds requiring further experimental validation.

---

## Key Syntax Differences: PySpark vs. Spark Scala

If you are transitioning from the Python SynapseML tutorial, keep these key differences in mind:

| Feature | Python (PySpark) | Scala (Spark) | Explanation |
| :--- | :--- | :--- | :--- |
| **Parameter Configuration** | `LightGBMRegressor(alpha=0.5, objective="quantile")` | `new LightGBMRegressor().setAlpha(0.5).setObjective("quantile")` | Scala uses the **fluent setter pattern** (`.setParam()`) instead of constructor keyword arguments. |
| **Variable Immutability** | `model = ...` | `val model = ...` | Scala uses `val` for immutable bindings and `var` for mutable variables. |
| **Array Definitions** | `[0.8, 0.2]` | `Array(0.8, 0.2)` | Scala uses typed collections (`Array(...)`, `Seq(...)`). |
| **Anonymous Functions** | `[c for c in cols if c != "label"]` | `cols.filter(_ != "label")` | Scala uses concise underscore `_` syntax for lambdas. |
| **Imports & Namespaces** | `import synapse.ml.lightgbm...` | `import com.microsoft.azure.synapse.ml.lightgbm...` | Scala follows full JVM package hierarchy namespaces. |

---

## Step 1: Environment Setup and Dependencies

To use LightGBM in Spark Scala, attach the SynapseML Maven coordinate to your Spark cluster or include it in your build configuration:

* **Maven Coordinate:** `com.microsoft.azure:synapseml_2.12:1.1.3`
* **Spark Packages:** `com.microsoft.azure:synapseml_2.12:1.1.3`
* **Repository:** `https://mmlspark.blob.core.windows.net/maven`

### Spark Shell / Databricks / Synapse Configuration
When launching `spark-shell` or `spark-submit`, include the package:
```bash
spark-shell --packages com.microsoft.azure:synapseml_2.12:1.1.3 \
            --repositories https://mmlspark.blob.core.windows.net/maven
```

---

## Step 2: Spark Session and Imports

Import the necessary classes from Spark SQL, Spark ML, and SynapseML:

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.RegressionEvaluator
import com.microsoft.azure.synapse.ml.lightgbm.{LightGBMRegressor, LightGBMRegressionModel}

// Initialize or retrieve the active SparkSession
val spark = SparkSession.builder()
  .appName("LightGBM-QSAR-QuantileRegression")
  .master("local[*]") // Use cluster master when deploying in production
  .getOrCreate()

import spark.implicits._
```

---

## Step 3: Dataset Preparation

In QSAR modeling, compounds are typically represented by physicochemical descriptors or molecular fingerprints (e.g., Molecular Weight, LogP, Hydrogen Bond Donors/Acceptors, Topological Polar Surface Area, Rotatable Bonds) mapped to a biological potency target (such as $pIC_{50} = -\log_{10}(IC_{50})$).

### Option A: Self-Contained Synthetic QSAR Dataset
To run this tutorial immediately without external network dependencies, generate a synthetic QSAR dataset:

```scala
case class CompoundDescriptor(
  compound_id: String,
  molecular_weight: Double,
  logP: Double,
  hbd_count: Double,
  hba_count: Double,
  tpsa: Double,
  rotatable_bonds: Double,
  pIC50: Double // Bioactivity potency target
)

// Generate sample molecular descriptor data with heteroscedastic noise
val random = new scala.util.Random(42)
val sampleCompounds = (1 to 500).map { i =>
  val mw = 200.0 + random.nextDouble() * 350.0       // Molecular Weight (Da)
  val logP = -0.5 + random.nextDouble() * 5.5        // Octanol-water partition coefficient
  val hbd = random.nextInt(6).toDouble              // Hydrogen Bond Donors
  val hba = random.nextInt(10).toDouble             // Hydrogen Bond Acceptors
  val tpsa = 20.0 + random.nextDouble() * 120.0     // Topological Polar Surface Area (Å²)
  val rotBonds = random.nextInt(8).toDouble         // Rotatable Bonds

  // Synthetic QSAR response with scaffold-dependent variance (heteroscedasticity)
  val latentPotency = 4.0 + (0.005 * mw) + (0.4 * logP) - (0.15 * hbd) - (0.01 * tpsa)
  val noiseScale = 0.2 + 0.1 * (logP.abs)           // Uncertainty increases with extreme logP
  val noise = random.nextGaussian() * noiseScale
  val potency = latentPotency + noise

  CompoundDescriptor(s"CMPD-$i", mw, logP, hbd, hba, tpsa, rotBonds, potency)
}

val qsarDf = sampleCompounds.toDF()
qsarDf.show(5, truncate = false)
```

### Option B: Public Triazines Benchmark Dataset (LibSVM)
SynapseML also hosts the classic benchmark Triazines QSAR dataset (predicting inhibition of dihydrofolate reductase by pyrimidines).

> **Note on LibSVM Schema:** The LibSVM format pre-assembles molecular features into a single Vector column (`features`) and maps the target property to `label`. As shown below, it does not require an intermediate `VectorAssembler` step and can be fed directly to `LightGBMRegressor`:

```scala
// Load benchmark Triazines QSAR dataset (requires cluster network connectivity)
val triazinesDf = spark.read
  .format("libsvm")
  .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/triazines.scale.svmlight")

println(s"Total records in Triazines dataset: ${triazinesDf.count()}")
triazinesDf.printSchema()

// Direct training on LibSVM's native columns without VectorAssembler:
val Array(triazinesTrain, triazinesTest) = triazinesDf.randomSplit(Array(0.8, 0.2), seed = 1234L)
val triazinesModel = new LightGBMRegressor()
  .setObjective("quantile")
  .setAlpha(0.5)
  .setLabelCol("label")
  .setFeaturesCol("features")
  .fit(triazinesTrain)

triazinesModel.transform(triazinesTest).select("label", "prediction").show(5)
```

> **Tutorial Flow:** The subsequent sections (Steps 4 through 7) follow **Option A (`qsarDf`)** to demonstrate how to perform custom feature engineering with `VectorAssembler`, multi-quantile uncertainty envelope modeling, and domain-specific bioactivity metric evaluation.

---

## Step 4: Feature Assembly & Train/Test Split

Assemble the molecular descriptor columns into a single Spark ML feature vector:

```scala
val featureCols = Array(
  "molecular_weight",
  "logP",
  "hbd_count",
  "hba_count",
  "tpsa",
  "rotatable_bonds"
)

val assembler = new VectorAssembler()
  .setInputCols(featureCols)
  .setOutputCol("features")

val assembledDf = assembler.transform(qsarDf)

// Split into training (80%) and testing (20%) datasets
val Array(trainData, testData) = assembledDf.randomSplit(Array(0.8, 0.2), seed = 1234L)
trainData.cache()
testData.cache()

println(s"Training set: ${trainData.count()} compounds")
println(s"Testing set:  ${testData.count()} compounds")
```

---

## Step 5: Training Multi-Quantile LightGBM Models

To construct an **uncertainty envelope**, train three separate `LightGBMRegressor` estimators configured with `setObjective("quantile")`:
* **$\alpha = 0.20$**: 20th percentile (conservative lower bound of compound potency).
* **$\alpha = 0.50$**: 50th percentile (median prediction, robust to outliers).
* **$\alpha = 0.80$**: 80th percentile (optimistic upper bound of compound potency).

```scala
// Helper method to create and configure a quantile regressor
def createQuantileRegressor(alpha: Double, predCol: String): LightGBMRegressor = {
  new LightGBMRegressor()
    .setObjective("quantile")
    .setAlpha(alpha)
    .setLabelCol("pIC50")
    .setFeaturesCol("features")
    .setPredictionCol(predCol)
    .setNumLeaves(31)
    .setNumIterations(100)
    .setLearningRate(0.05)
    .setMinDataInLeaf(10)
    .setSeed(42)
}

println("Training 20th percentile (Lower Bound) model...")
val modelQ20 = createQuantileRegressor(0.20, "pred_q20").fit(trainData)

println("Training 50th percentile (Median) model...")
val modelQ50 = createQuantileRegressor(0.50, "pred_q50_median").fit(trainData)

println("Training 80th percentile (Upper Bound) model...")
val modelQ80 = createQuantileRegressor(0.80, "pred_q80").fit(trainData)
```

---

## Step 6: Generating the Uncertainty Envelope

Transform the test data sequentially through all three models, then calculate the **uncertainty width** ($q_{80} - q_{20}$):

```scala
val predictions = modelQ80.transform(
  modelQ50.transform(
    modelQ20.transform(testData)
  )
)

// Calculate prediction interval width (uncertainty) and interval coverage
val predictionsWithInterval = predictions.withColumn(
  "uncertainty_width",
  $"pred_q80" - $"pred_q20"
).withColumn(
  "within_interval",
  $"pIC50" >= $"pred_q20" && $"pIC50" <= $"pred_q80"
)

// Display sample predictions with uncertainty bounds
predictionsWithInterval
  .select("compound_id", "pIC50", "pred_q20", "pred_q50_median", "pred_q80", "uncertainty_width", "within_interval")
  .show(10, truncate = false)
```

### Interpretation for Medicinal Chemists
* **Small `uncertainty_width`:** The model is confident; the molecule's chemical features lie in well-sampled chemical space.
* **Large `uncertainty_width`:** High epistemic or assay uncertainty; proceed with caution before ordering synthesis.
* **High `pred_q20`:** Even under pessimistic estimation, the molecule exhibits strong potency—ideal for prioritization.

---

## Step 7: Model Evaluation & Validation

Evaluate the median model using standard regression metrics (RMSE and MAE) via `RegressionEvaluator`, and compute empirical coverage:

```scala
// 1. Evaluate Median Model RMSE
val rmseEvaluator = new RegressionEvaluator()
  .setLabelCol("pIC50")
  .setPredictionCol("pred_q50_median")
  .setMetricName("rmse")

val rmse = rmseEvaluator.evaluate(predictionsWithInterval)
println(f"Median Model RMSE: $rmse%.4f")

// 2. Evaluate Median Model MAE
val maeEvaluator = new RegressionEvaluator()
  .setLabelCol("pIC50")
  .setPredictionCol("pred_q50_median")
  .setMetricName("mae")

val mae = maeEvaluator.evaluate(predictionsWithInterval)
println(f"Median Model MAE:  $mae%.4f")

// 3. Empirical Interval Coverage (Nominal target: 80% - 20% = 60%)
val coverageCount = predictionsWithInterval.filter($"within_interval" === true).count()
val totalCount = predictionsWithInterval.count()
val empiricalCoverage = (coverageCount.toDouble / totalCount.toDouble) * 100.0

println(f"Empirical Coverage: $empiricalCoverage%.2f%% (Nominal target: 60.00%%)")
```

---

## Step 8: Standalone Spark Scala Application (`spark-submit`)

To package this workflow into a standalone Scala application as requested in [#731](https://github.com/microsoft/SynapseML/issues/731), organize the project with sbt:

### 1. `build.sbt`
```scala
name := "synapseml-lightgbm-qsar-standalone"
version := "1.0.0"
scalaVersion := "2.12.17"
resolvers += "SynapseML Maven Repo" at "https://mmlspark.blob.core.windows.net/maven"

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "com.microsoft.azure" % "synapseml_2.12" % "1.1.3"
)
```

### 2. Standalone Application (`QSARQuantileApp.scala`)
```scala
package com.example.drugdiscovery

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.RegressionEvaluator
import com.microsoft.azure.synapse.ml.lightgbm.LightGBMRegressor

object QSARQuantileApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("QSAR-Quantile-Regression-Standalone")
      .getOrCreate()

    import spark.implicits._

    println("=== Running SynapseML LightGBM Quantile Regression Pipeline ===")

    // 1. Generate Synthetic Data
    val random = new scala.util.Random(42)
    val data = (1 to 1000).map { i =>
      val mw = 150.0 + random.nextDouble() * 400.0
      val logP = -1.0 + random.nextDouble() * 6.0
      val hbd = random.nextInt(5).toDouble
      val hba = random.nextInt(9).toDouble
      val potency = 5.0 + 0.004 * mw + 0.35 * logP - 0.1 * hbd + random.nextGaussian() * 0.25
      (s"MOL_$i", mw, logP, hbd, hba, potency)
    }.toDF("id", "mw", "logP", "hbd", "hba", "potency")

    // 2. Assemble Features
    val assembler = new VectorAssembler()
      .setInputCols(Array("mw", "logP", "hbd", "hba"))
      .setOutputCol("features")

    val assembled = assembler.transform(data)
    val Array(train, test) = assembled.randomSplit(Array(0.8, 0.2), 42L)

    // 3. Train Quantile Models (10th, 50th, 90th percentiles for an 80% confidence band)
    val quantiles = Seq(
      (0.10, "pred_lower_10"),
      (0.50, "pred_median_50"),
      (0.90, "pred_upper_90")
    )

    var scoredTest = test
    for ((alpha, predCol) <- quantiles) {
      val model = new LightGBMRegressor()
        .setObjective("quantile")
        .setAlpha(alpha)
        .setLabelCol("potency")
        .setFeaturesCol("features")
        .setPredictionCol(predCol)
        .setNumLeaves(31)
        .setNumIterations(50)
        .setLearningRate(0.05)
        .fit(train)

      scoredTest = model.transform(scoredTest)
    }

    // 4. Compute Metrics
    val evaluator = new RegressionEvaluator()
      .setLabelCol("potency")
      .setPredictionCol("pred_median_50")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(scoredTest)
    println(f"Median Model RMSE: $rmse%.4f")

    scoredTest.select("id", "potency", "pred_lower_10", "pred_median_50", "pred_upper_90")
      .show(5, truncate = false)

    spark.stop()
  }
}
```

### 3. Execution via `spark-submit`
```bash
# Package the application
sbt package

# Submit to Spark cluster
spark-submit \
  --class com.example.drugdiscovery.QSARQuantileApp \
  --master yarn \
  --deploy-mode client \
  --packages com.microsoft.azure:synapseml_2.12:1.1.3 \
  --repositories https://mmlspark.blob.core.windows.net/maven \
  target/scala-2.12/synapseml-lightgbm-qsar-standalone_2.12-1.0.0.jar
```

---

## Summary

In this guide, you learned how to:
1. Configure SynapseML LightGBM in Apache Spark Scala using current coordinates (`1.1.3`).
2. Translate PySpark syntax to idiomatic Scala using fluent setter methods (`.setParam()`).
3. Model biological activity ($pIC_{50}$) with Quantile Regression to estimate uncertainty intervals ($q_{20}, q_{50}, q_{80}$).
4. Calculate empirical coverage and evaluate prediction accuracy with Spark ML's `RegressionEvaluator`.
5. Package and submit a standalone Spark Scala LightGBM application using `sbt` and `spark-submit`.


