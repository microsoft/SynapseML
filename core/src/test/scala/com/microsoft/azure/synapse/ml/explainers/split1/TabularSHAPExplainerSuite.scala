// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.core.utils.BreezeUtils._
import com.microsoft.azure.synapse.ml.explainers.LocalExplainer.KernelSHAP
import com.microsoft.azure.synapse.ml.explainers.{KernelSHAPTabularSampler, TabularSHAP}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{OneHotEncoder, SQLTransformer, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{array, avg, broadcast, lit}
import org.apache.spark.sql.types._
import org.scalactic.{Equality, TolerantNumerics}

import java.util.concurrent.atomic.AtomicLong

class TabularSHAPExplainerSuite extends TestBase
  with TransformerFuzzing[TabularSHAP] {

  import spark.implicits._

  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1E-5)

  override val sortInDataframeEquality = true

  lazy val data: DataFrame = (1 to 100).flatMap(_ => Seq(
    (-5d, "a", -5d, 0),
    (-5d, "b", -5d, 0),
    (5d, "a", 5d, 1),
    (5d, "b", 5d, 1)
  )).toDF("col1", "col2", "col3", "label")

  lazy val pipeline: Pipeline = new Pipeline().setStages(Array(
    new StringIndexer().setInputCol("col2").setOutputCol("col2_ind"),
    new OneHotEncoder().setInputCol("col2_ind").setOutputCol("col2_enc"),
    new VectorAssembler().setInputCols(Array("col1", "col2_enc", "col3")).setOutputCol("features"),
    new LogisticRegression().setLabelCol("label").setFeaturesCol("features")
  ))

  lazy val model: PipelineModel = pipeline.fit(data)

  lazy val kernelShap: TabularSHAP = KernelSHAP.tabular
    .setInputCols(Array("col1", "col2", "col3"))
    .setOutputCol("shapValues")
    .setBackgroundData(data)
    .setNumSamples(1000)
    .setModel(model)
    .setTargetCol("probability")
    .setTargetClasses(Array(1))

  lazy val infer: DataFrame = Seq(
    (3d, "a", 3d)
  ) toDF("col1", "col2", "col3")

  test("TabularKernelSHAP can explain a model locally") {
    val coefficients = model.stages.last.asInstanceOf[LogisticRegressionModel].coefficients.toBreeze

    assert(coefficients(0) === 1.9099146508533622)
    assert(coefficients(1) === 0d)
    assert(coefficients(2) === 1.9099146508533622)

    val predicted = model.transform(infer)

    val (probability, shapValues, r2) = kernelShap
      .transform(predicted)
      .select("probability", "shapValues", "r2").as[(Vector, Seq[Vector], Vector)]
      .head

    val shapBz = shapValues.head.toBreeze
    val avgLabel = model.transform(data).select(avg("prediction")).as[Double].head

    // Base value (weightsBz(0)) should match average label from background data set.
    assert(shapBz(0) === avgLabel)

    // Sum of shap values should match prediction
    assert(probability(1) === breeze.linalg.sum(shapBz))

    // Null feature (col2) should have zero shap values
    assert(Math.abs(shapBz(2)) < 1E-5)

    // col1 and col3 are symmetric so they should have same shap values.
    assert(shapBz(1) === shapBz(3))

    // R-squared of the underlying regression should be close to 1.
    assert(r2(0) === 1d)
  }

  test("TabularSHAP sample expansion emits rows without buffering a pair's sample budget") {
    val numFeatures = 14
    val numSamples = 10000
    var pairsPulled = 0
    var samplesDrawn = 0
    val instance = Row.fromSeq(Seq.fill(numFeatures)(1d))
    val background = Row.fromSeq(Seq.fill(numFeatures)(0d))
    val pairs = Iterator(0L, 1L).map {
      id =>
        pairsPulled += 1
        Row(id, Seq(1), instance, background)
    }

    val rows = TabularSHAP.expandPairs(pairs, numSamples) {
      (i, b) =>
        new KernelSHAPTabularSampler(i, b, numSamples, 1E8) {
          override def sample: (Row, Vector, Double) = {
            samplesDrawn += 1
            super.sample
          }
        }
    }

    assert(samplesDrawn === 0)
    val first = rows.next()
    assert(samplesDrawn === 1)
    assert(pairsPulled === 1)
    assert(first.length === numFeatures + 4)
    assert(first.getLong(0) === 0L)
    assert(rows.take(9).size === 9)
    assert(samplesDrawn === 10)
    assert(pairsPulled === 1)
  }

  test("TabularKernelSHAP accepts nullable background values") {
    val instances = spark.range(3).selectExpr(
      "cast(id as double) as x",
      "cast(id * 2 as double) as y"
    )
    val background = Seq(
      (Option.empty[Double], Option(1d)),
      (Option(2d), Option.empty[Double])
    ).toDF("x", "y")
    val additiveModel = new SQLTransformer().setStatement(
      "SELECT *, cast(coalesce(x, 10.0) + coalesce(y, 100.0) AS DOUBLE) AS score FROM __THIS__"
    )

    val results = KernelSHAP.tabular
      .setInputCols(Array("x", "y"))
      .setOutputCol("shapValues")
      .setBackgroundData(background)
      .setModel(additiveModel)
      .setTargetCol("score")
      .transform(instances)
      .orderBy("x")
      .select("shapValues")
      .as[Seq[Vector]]
      .collect()

    assert(results.length === 3)
    results.zipWithIndex.foreach {
      case (Seq(values), x) =>
        assert(values(0) === 56.5)
        assert(values(1) === x.toDouble - 6d)
        assert(values(2) === 2d * x - 50.5)
    }
  }

  test("TabularSHAP createSamples streams encoded rows before expanding the sample budget") {
    import TabularSHAPExplainerSuite._
    val numSamples = 10000
    val features = (0 until 14).map(i => s"f$i").toArray
    val schema = StructType(Seq(StructField(features.head, new CountedValueUDT)) ++
      features.tail.map(StructField(_, DoubleType)))
    val instance = Row.fromSeq(Seq(CountedValue(1)) ++ Seq.fill(13)(1d))
    val backgroundRow = Row.fromSeq(Seq(CountedValue(0)) ++ Seq.fill(13)(0d))
    val instances = spark.createDataFrame(java.util.Collections.singletonList(instance), schema)
      .withColumn("id", lit(0L)).withColumn("targetClasses", array(lit(1)))
    val background = spark.createDataFrame(java.util.Arrays.asList(backgroundRow, backgroundRow), schema)
    val samples = new SampleAccess()
      .setInputCols(features).setNumSamples(numSamples).setBackgroundData(broadcast(background))
      .samples(instances)

    Serialized.set(0)
    val rows = samples.limit(1).collect()
    assert(rows.length === 1)
    assert(rows.head.getAs[CountedValue]("f0").value === 0)
    assert(rows.head.getAs[Vector]("coalition").toArray.forall(_ == 0d))
    assert(rows.head.getAs[Double]("weight") === 1E8)
    assert(Serialized.get() > 0 && Serialized.get() < numSamples,
      s"Encoded ${Serialized.get()} feature values before returning the first sample")
  }

  test("TabularKernelSHAP preserves nulls inside background features") {
    val featureShapes = Seq(
      ("named_struct('value', %s)", "x.value"),
      ("array(%s)", "x[0]"),
      ("map('value', %s)", "x['value']"),
      ("map('outer', array(named_struct('value', %s)))", "x['outer'][0].value")
    )
    featureShapes.foreach {
      case (shape, access) =>
        val instances = spark.range(1, 2).selectExpr(
          s"${shape.format("cast(id as double)")} as x", "cast(id * 2 as double) as y")
        val background = spark.range(1).selectExpr(
          s"${shape.format("cast(null as double)")} as x", "cast(id as double) as y")
        val additiveModel = new SQLTransformer().setStatement(
          s"SELECT *, cast(coalesce($access, 10.0) + y AS DOUBLE) AS score FROM __THIS__")

        val results = KernelSHAP.tabular
          .setInputCols(Array("x", "y"))
          .setOutputCol("shapValues")
          .setBackgroundData(background)
          .setModel(additiveModel)
          .setTargetCol("score")
          .transform(instances)
        try {
          val values = results.select("shapValues").as[Seq[Vector]].collect()
          assert(values.length === 1)
          assert(values.head.length === 1)
          assert(values.head.head(0) === 10d)
          assert(values.head.head(1) === -9d)
          assert(values.head.head(2) === 2d)
        } finally {
          spark.catalog.clearCache()
        }
    }
  }

  override def testObjects(): Seq[TestObject[TabularSHAP]] = Seq(new TestObject(kernelShap, infer))

  override def reader: MLReadable[_] = TabularSHAP
}

object TabularSHAPExplainerSuite {
  // The local Spark test counts encoding at the production sampling boundary, not just iterator calls.
  private val Serialized = new AtomicLong()

  @SQLUserDefinedType(udt = classOf[CountedValueUDT])
  case class CountedValue(value: Int)

  class CountedValueUDT extends UserDefinedType[CountedValue] {
    override def sqlType: DataType = IntegerType
    override def userClass: Class[CountedValue] = classOf[CountedValue]
    override def serialize(value: CountedValue): Any = {
      Serialized.incrementAndGet()
      value.value
    }
    override def deserialize(value: Any): CountedValue = CountedValue(value.asInstanceOf[Int])
  }

  private class SampleAccess extends TabularSHAP {
    def samples(df: DataFrame): DataFrame = createSamples(df, "id", "coalition", "weight", "targetClasses")
  }
}
