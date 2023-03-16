// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers.split1

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import com.microsoft.azure.synapse.ml.explainers.{ICECategoricalFeature, ICENumericFeature, ICETransformer}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.jdk.CollectionConverters._


class ICEExplainerSuite extends TestBase with TransformerFuzzing[ICETransformer] {

  import spark.implicits._
  lazy val dataDF: DataFrame = (1 to 100).flatMap(_ => Seq(
    (-5, "a", -5, 0),
    (-5, "b", -5, 0),
    (5, "a", 5, 1),
    (5, "b", 5, 1)
  )).toDF("col1", "col2", "col3", "label")

  lazy val data: DataFrame = dataDF.withColumn("col4", rand()*100)  //scalastyle:ignore magic.number

  lazy val pipeline: Pipeline = new Pipeline().setStages(Array(
    new StringIndexer().setInputCol("col2").setOutputCol("col2_ind"),
    new OneHotEncoder().setInputCol("col2_ind").setOutputCol("col2_enc"),
    new VectorAssembler().setInputCols(Array("col1", "col2_enc", "col3", "col4")).setOutputCol("features"),
    new LogisticRegression().setLabelCol("label").setFeaturesCol("features")
  ))
  lazy val model: PipelineModel = pipeline.fit(data)

  lazy val ice: ICETransformer = new ICETransformer()
    .setModel(model)
    .setTargetCol("probability")
    .setCategoricalFeatures(Array(ICECategoricalFeature("col2", Some(2)), ICECategoricalFeature("col3", Some(4))))
    .setTargetClasses(Array(1))
  lazy val output: DataFrame = ice.transform(data).cache()

  lazy val iceAvg: ICETransformer = new ICETransformer()
    .setModel(model)
    .setTargetCol("probability")
    .setCategoricalFeatures(Array(ICECategoricalFeature("col1", Some(100)), ICECategoricalFeature("col2"),
      ICECategoricalFeature("col3")))
    .setNumericFeatures(Array(ICENumericFeature("col4", Some(5))))
    .setTargetClasses(Array(1))
    .setKind("average")
  lazy val outputAvg: DataFrame = iceAvg.transform(data).cache()

  lazy val iceFeat: ICETransformer = new ICETransformer()
    .setModel(model)
    .setTargetCol("probability")
    .setCategoricalFeatures(Array(ICECategoricalFeature("col1", Some(100)), ICECategoricalFeature("col2"),
     ICECategoricalFeature("col3")))
    .setNumericFeatures(Array(ICENumericFeature("col4", Some(5))))
    .setTargetClasses(Array(1))
    .setKind("feature")
  lazy val outputFeat: DataFrame = iceFeat.transform(data).cache()
  // Output schema: number_of_features rows * 2 cols (name of the feature + corresponding dependence)
  // Output is sorted by dependence in descending order
  // For this example you should expect:
  //  outputFeat.show(false)
 //  +------------------------+---------------+
//  |pdpBasedDependence      |featureNames   |
//  +------------------------+---------------+
//  |[0.1249879667776877]    |col1_dependence|
//    |[0.1249879667776877]    |col3_dependence|
//    |[7.096582491573019E-11] |col4_dependence|
//    |[1.1503853425409716E-12]|col2_dependence|
//    +------------------------+---------------+



  // Helper function which returns value from first row in a column specified by "colName".
  def getFirstValueFromOutput(output: DataFrame, colName: String): Map[_, Vector] = {
    output.select(colName).collect().map(_.getAs[Map[_, Vector]](0)).head
  }

  test("col2 doesn't contribute to the prediction.") {
    val outputCol2: Map[String, Vector] =
      getFirstValueFromOutput(outputAvg, "col2_dependence").asInstanceOf[Map[String, Vector]]

    val impA: Double = outputCol2.get("a").head.toArray.head
    val impB: Double = outputCol2.get("b").head.toArray.head
    val eps = 0.01
    assert((impA - impB).abs < eps)
  }

  test("col3 contribute to the prediction.") {

    val outputCol3: Map[Int, Vector] =
      getFirstValueFromOutput(outputAvg, "col3_dependence").asInstanceOf[Map[Int, Vector]]

    val impFirst: Double = outputCol3.get(-5).head.toArray.head
    val impSec: Double = outputCol3.get(5).head.toArray.head
    assert((impFirst - impSec).abs > 0.4)
  }

  test("The length of explainer map for numeric feature is equal to it's numSplits.") {

    val outputCol1: Map[Double, Vector] =
      getFirstValueFromOutput(outputAvg, "col4_dependence").asInstanceOf[Map[Double, Vector]]

    assert(outputCol1.size == iceAvg.getNumericFeatures.head.getNumSplits + 1)
  }

  test("The length of explainer map for categorical feature is less or equal to it's numTopValues.") {
    val outputCol: Map[Double, Vector] =
      getFirstValueFromOutput(output, "col3_dependence").asInstanceOf[Map[Double, Vector]]

    assert(outputCol.size <= ice.getCategoricalFeatures.last.getNumTopValue)
  }

  test("No features specified.") {
    val ice: ICETransformer = new ICETransformer()
    ice.setModel(model)
      .setTargetCol("probability")
      .setTargetClasses(Array(1))
    assertThrows[Exception](ice.transform(data))
  }

  test("Duplicate features specified.") {
    val ice: ICETransformer = new ICETransformer()
    ice.setModel(model)
      .setTargetCol("probability")
      .setCategoricalFeatures(Array(ICECategoricalFeature("col1", Some(100)),
        ICECategoricalFeature("col2"), ICECategoricalFeature("col1")))
      .setTargetClasses(Array(1))
    assertThrows[Exception](ice.transform(data))
  }

  test("When setNumSamples is called, ICE returns correct number of rows.") {
    val ice: ICETransformer = new ICETransformer()
    ice.setNumSamples(2)
      .setModel(model)
      .setTargetCol("probability")
      .setCategoricalFeatures(Array(ICECategoricalFeature("col2", Some(2)), ICECategoricalFeature("col3", Some(4))))
      .setTargetClasses(Array(1))
    val output = ice.transform(data)
    assert(output.count() == 2)
  }

  test("ICECategoricalFeature is successfully created from java.util.Map") {
    val map = new java.util.HashMap[String, Any]()
    map.put("name", "my_name")
    map.put("numTopValues", 100)
    val feature = ICECategoricalFeature.fromMap(map)
    assert(feature.name == map.get("name"))
    assert(feature.numTopValues.contains(map.get("numTopValues")))
    assert(feature.outputColName.isEmpty)
  }

  test("Set categorical") {
    val map = new java.util.HashMap[String, Any]()
    map.put("name", "col2")
    map.put("numTopValues", 2)
    val feature = ICECategoricalFeature.fromMap(map)
    ice.setCategoricalFeaturesPy(List(map).asJava)
    assert(ice.getCategoricalFeatures.head == feature)
  }

  test("For kind `feature` output has a number of rows equal to the number of passed features") {
    val numFeatures = iceFeat.getCategoricalFeatures.size + iceFeat.getNumericFeatures.size
    assert(outputFeat.select("featureNames").collect().length == numFeatures)
  }

  override def testObjects(): Seq[TestObject[ICETransformer]] = Seq(new TestObject(ice, data))
  override def reader: MLReadable[_] = ICETransformer
}
