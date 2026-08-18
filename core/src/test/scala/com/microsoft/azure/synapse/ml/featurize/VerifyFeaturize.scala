// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.featurize

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkException
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.feature.{StringIndexer, VectorSizeHint, VectorSlicer}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql._

import java.io.File
import java.lang.{Double => JDouble}
import java.nio.file.Files
import java.sql.{Date, Timestamp}
import java.util.GregorianCalendar

//scalastyle:off null
class VerifyFeaturize extends TestBase with EstimatorFuzzing[Featurize] {

  val mockLabelColumn = "Label"
  val featuresColumn = "testColumn"

  lazy val resourcesDirectory = new File(getClass.getResource("/").toURI)
  lazy val oldBenchmarkDir = new File(resourcesDirectory, "benchmarks")
  lazy val newBenchmarkDir = new File(resourcesDirectory, "new_benchmarks")

  def getResource(name: String): File = {
    new File(oldBenchmarkDir, name)
  }

  private def getTempFile(name: String): File = {
    new File(newBenchmarkDir, name)
  }

  lazy val benchmarkBasicDataTypesFile = "benchmarkBasicDataTypes.json"
  lazy val historicDataTypesFile: File = getResource(benchmarkBasicDataTypesFile)

  lazy val benchmarkVectorsFile = "benchmarkVectors.json"
  lazy val historicVectorsFile: File = getResource(benchmarkVectorsFile)

  lazy val benchmarkStringFile = "benchmarkString.json"
  lazy val historicStringFile: File = getResource(benchmarkStringFile)

  lazy val benchmarkStringMissingsFile = "benchmarkStringMissing.json"
  lazy val historicStringMissingsFile: File = getResource(benchmarkStringMissingsFile)

  lazy val benchmarkOneHotFile = "benchmarkOneHot.json"
  lazy val historicOneHotFile: File = getResource(benchmarkOneHotFile)

  lazy val benchmarkNoOneHotFile = "benchmarkNoOneHot.json"
  lazy val historicNoOneHotFile: File = getResource(benchmarkNoOneHotFile)

  lazy val benchmarkOneHotMissingsFile = "benchmarkOneHotMissings.json"
  lazy val historicOneHotMissingsFile: File = getResource(benchmarkOneHotMissingsFile)

  lazy val benchmarkNoOneHotMissingsFile = "benchmarkNoOneHotMissings.json"
  lazy val historicNoOneHotMissingsFile: File = getResource(benchmarkNoOneHotMissingsFile)

  lazy val benchmarkStringIndexOneHotFile = "benchmarks/benchmarkStringIndexOneHot.json"
  lazy val historicStringIndexOneHotFile: File = getResource(benchmarkStringIndexOneHotFile)

  lazy val benchmarkDateFile = "benchmarkDate.json"
  lazy val historicDateFile: File = getResource(benchmarkDateFile)

  // int label with features of:
  // long, double, boolean, int, byte, float
  lazy val mockDataset = spark.createDataFrame(Seq(
    (0, 2L, 0.50, true, 0, 0.toByte, 12F),
    (1, 3L, 0.40, false, 1, 100.toByte, 30F),
    (0, 4L, 0.78, true, 2, 50.toByte, 12F),
    (1, 5L, 0.12, false, 3, 0.toByte, 12F),
    (0, 1L, 0.50, true, 0, 0.toByte, 30F),
    (1, 3L, 0.40, false, 1, 10.toByte, 12F),
    (0, 3L, 0.78, false, 2, 0.toByte, 12F),
    (1, 4L, 0.12, false, 3, 0.toByte, 12F),
    (0, 0L, 0.50, true, 0, 0.toByte, 12F),
    (1, 2L, 0.40, false, 1, 127.toByte, 30F),
    (0, 3L, 0.78, true, 2, -128.toByte, 12F),
    (1, 4L, 0.12, false, 3, 0.toByte, 12F)))
    .toDF(mockLabelColumn, "col1", "col2", "col3", "col4", "col5", "col6")

  test("Featurizing on some basic data types") {
    val result: DataFrame = featurizeAndVerifyResult(mockDataset, historicDataTypesFile)
    // Verify that features column has the correct number of slots
    assert(result.first().getAs[DenseVector](featuresColumn).values.length == 6)
  }

  test("Featurizing with vector columns, sparse and dense") {
    val dataset: DataFrame = spark.createDataFrame(Seq(
      (0, Vectors.sparse(3, Seq((0, 1.0), (2, 2.0))), 0.50, 0.60, 0, Vectors.dense(1.0, 0.1, -1.5)),
      (1, Vectors.dense(1.5, 0.2, -1.2), 0.40, 0.50, 1, Vectors.dense(1.5, 0.2, -1.2)),
      (1, Vectors.sparse(3, Seq((0, 1.0), (2, 2.0))), 0.12, 0.34, 3, Vectors.sparse(3, Seq((0, 1.0), (2, 2.0)))),
      (0, Vectors.dense(1.1, 0.5, -1.024), 0.50, 0.60, 0, Vectors.dense(1.0, 0.4, -1.23)),
      (1, Vectors.dense(1.1, 0.5, -1.056), 0.40, 0.50, 1, Vectors.dense(1.1, 0.5, -1.024)),
      (0, Vectors.dense(Double.NaN, 0.2, -1.23), 0.78, 0.99, 2, Vectors.dense(1.0, 0.1, -1.22)),
      (1, Vectors.dense(1.0, 0.4, -1.23), 0.12, 0.34, 3, Vectors.dense(Double.NaN, 0.2, -1.23))))
      .toDF(mockLabelColumn, "col1", "col2", "col3", "col4", "col5")

    val result: DataFrame = featurizeAndVerifyResult(dataset, historicVectorsFile)
    // Verify that features column has the correct number of slots
    assert(result.first().getAs[DenseVector](featuresColumn).values.length == 9)
  }

  test("Featurizing with text columns - using hashing with count based feature selection") {
    val dataset: DataFrame = spark.createDataFrame(Seq(
      (0, 2, 0.50, 0.60, "pokemon are everywhere"),
      (1, 3, 0.40, 0.50, "they are in the woods"),
      (0, 4, 0.78, 0.99, "they are in the water"),
      (1, 5, 0.12, 0.34, "they are in the fields"),
      (0, 3, 0.78, 0.99, "pokemon - gotta catch em all")))
      .toDF(mockLabelColumn, "col1", "col2", "col3", "col4")

    val result: DataFrame = featurizeAndVerifyResult(dataset, historicStringFile)
    // Verify that features column has the correct number of slots
    assert(result.first().getAs[SparseVector](featuresColumn).size == 9)
  }

  test("Featurizing with date and timestamp columns") {
    val dataset: DataFrame = spark.createDataFrame(Seq(
      (0, 2, 0.50, 0.60, new Date(new GregorianCalendar(2017, 6, 7).getTimeInMillis), new Timestamp(1000)),
      (1, 3, 0.40, 0.50, new Date(new GregorianCalendar(2017, 6, 8).getTimeInMillis), new Timestamp(2000)),
      (0, 4, 0.78, 0.99, new Date(new GregorianCalendar(2017, 6, 6).getTimeInMillis), new Timestamp(3000)),
      (1, 5, 0.12, 0.34, new Date(new GregorianCalendar(2016, 6, 5).getTimeInMillis), new Timestamp(4000)),
      (0, 3, 0.78, 0.99, new Date(new GregorianCalendar(2010, 6, 9).getTimeInMillis), new Timestamp(5000))))
      .toDF(mockLabelColumn, "col1", "col2", "col3", "date", "timestamp")

    val result: DataFrame = featurizeAndVerifyResult(dataset, historicDateFile)
    // Verify that features column has the correct number of slots
    assert(result.first().getAs[DenseVector](featuresColumn).size == 16)
  }

  test("Verify featurizing text data produces proper tokenized output") {
    val wordCountCol = "wordCount"
    val wordLengthCol = "wordLength"
    val textCol = "textCol"
    val mockAmazonData = spark.createDataFrame(Seq(
      (1, 221, 4.42, "Ok~ but I think the Keirsey Temperment Test is more accurate - and cheaper.  This book has its " +
        "good points. If anything, it helps you put into words what you want  from a supervisor, but it is not very " +
        "accurate. The online test does not account for a difference between when 2 of their options are both " +
        "exactly like you, or if they don't describe you at all. This messes up the results, and it did not " +
        "describe me very well. I am not just in denial. I have taken a lot of personality type tests, like " +
        "the Keirsey Temperment sorter and have a pretty good idea of my strengths. So, although this book is " +
        "pretty good in making you understand the importance of incouraging your strengths, it still " +
        "leaves you wondering about how you fit in to their terminology.  As for using this book as a manager " +
        "to better serve your employees, I'v seen it done and it does not necessarily work because the strengths " +
        "spit out for people were not wholly accurate. The company I work for has done this, and most of the " +
        "people who were shifted around to better serve their strengths (according to this book) are very " +
        "unhappy in their new positions.  Your money can be spent better elsewhere. I say its only worth about $10"),
      (0, 138, 4.49, "I had a bad feeling about this!  And I was right!  I was intrigued by the title, which " +
        "supposedly links Jedi wisdom to Christianity.  Well, after 60 pages or so, I have got the feeling that the " +
        "Staub is trying to wrap Jedi in Christian cloth and failing at that. The author speaks of the difficulty in " +
        "leading a Christian life.  But, I say that any religious life (be it Christian, Islam or otherwise) is hard " +
        "because it turns the back on the norm or the conventional.   I am convinced that Yoda is a Zen master; " +
        "the Force is derived from Tao, not God as interpreted by the orthodox religion(I am purposefully leaving " +
        "out Christian Mysticism, which is another beast altogether.). A better book on the subject of theology " +
        "in Star wars is \"The Dharma of Star Wars.\""),
      (0, 43, 4.98, "Poorly written  I tried reading this book but found it so turgid and poorly written that I " +
        "put it down in frustration.  It reads like a translation from another language by an academic bureacrat. " +
        "The theme is interesting, the execution poor.  Cannot recommend")))
      .toDF(mockLabelColumn, wordCountCol, wordLengthCol, textCol)

    val featModel = new Featurize()
      .setInputCols(Array(wordCountCol, wordLengthCol, textCol))
      .setOutputCol(featuresColumn)
      .setNumFeatures(100000).fit(mockAmazonData)
    val nonzeroValuesThreshold = 30
    featModel.transform(mockAmazonData).collect().foreach(
      row => assert(row.getAs[SparseVector](featuresColumn).indices.length >= nonzeroValuesThreshold,
        "Strings improperly tokenized")
    )
  }

  test("Featurizing with text columns that have missing values - " +
    "using hashing with count based feature selection") {
    val dataset: DataFrame = spark.createDataFrame(Seq(
      (0, 2, 0.50, "pokemon are everywhere"),
      (1, 3, 0.40, null),
      (0, 4, 0.78, "they are in the water"),
      (1, 5, 0.12, "they are in the fields"),
      (0, 3, 0.78, null)))
      .toDF(mockLabelColumn, "col1", "col2", "col3")

    val result: DataFrame = featurizeAndVerifyResult(dataset, historicStringMissingsFile)
    // Verify that features column has the correct number of slots
    assert(result.first().getAs[Vector](featuresColumn).size == 8)
  }

  test("Featurizing with categorical columns - using one hot encoding") {
    val cat = "Cat"
    val dog = "Dog"
    val bird = "Bird"
    val dataset: DataFrame = spark.createDataFrame(Seq(
      (0, 2, 0.50, 0.60, dog, cat),
      (1, 3, 0.40, 0.50, cat, dog),
      (0, 4, 0.78, 0.99, dog, bird),
      (1, 5, 0.12, 0.34, cat, dog),
      (0, 3, 0.78, 0.99, dog, bird),
      (1, 4, 0.12, 0.34, bird, dog)))
      .toDF(mockLabelColumn, "col1", "col2", "col3", "col4", "col5")

    val model1 = new ValueIndexer().setInputCol("col4").setOutputCol("col4").fit(dataset)
    val model2 = new ValueIndexer().setInputCol("col5").setOutputCol("col5").fit(dataset)
    val catDataset = model1.transform(model2.transform(dataset))

    val result: DataFrame = featurizeAndVerifyResult(catDataset, historicOneHotFile,
      oneHotEncode = true)
    // Verify that features column has the correct number of slots
    assert(result.first().getAs[DenseVector](featuresColumn).size == 7)

    // Verify without one-hot encoding we get expected data
    val resultNoOneHot: DataFrame = featurizeAndVerifyResult(catDataset, historicNoOneHotFile)
    // Verify that features column has the correct number of slots
    assert(resultNoOneHot.first().getAs[DenseVector](featuresColumn).size == 5)

    // Verify get equivalent results if we use string indexer for making categoricals
    val tmp4col = "col4tmp"
    val tmp5col = "col5tmp"
    val strind1 = new StringIndexer().setInputCol("col4").setOutputCol(tmp4col)
    val strind2 = new StringIndexer().setInputCol("col5").setOutputCol(tmp5col)
    val fit1 = strind1.fit(dataset)
    val catResult1 = fit1.transform(dataset)
    val fit2 = strind2.fit(catResult1)
    val catResult2 = fit2.transform(catResult1)
      .drop("col4", "col5")
      .withColumnRenamed(tmp4col, "col4")
      .withColumnRenamed(tmp5col, "col5")

    val resultStringIndexer: DataFrame = featurizeAndVerifyResult(catResult2, historicStringIndexOneHotFile,
      oneHotEncode = true)
    // Verify that features column has the correct number of slots
    assert(resultStringIndexer.first().getAs[DenseVector](featuresColumn).size == 7)
  }

  test("issue 1667 - ordinary text featurization is unchanged when constant text columns are present") {
    val trainingDataset = spark.createDataFrame(Seq(
      (0, 1.0, "always the same", "red fox"),
      (1, 2.0, "always the same", "blue fox"),
      (0, 3.0, "always the same", "red dog"),
      (1, 4.0, "always the same", "blue dog")))
      .toDF(mockLabelColumn, "numeric", "constantText", "informativeText")
    val featurizeUid = "issue1667Featurize"

    val baseModel = new Featurize(featurizeUid)
      .setNumFeatures(1024)
      .setOutputCol(featuresColumn)
      .setInputCols(Array("informativeText", "numeric"))
      .fit(trainingDataset)

    val modelWithConstantText = new Featurize(featurizeUid)
      .setNumFeatures(1024)
      .setOutputCol(featuresColumn)
      .setInputCols(Array("constantText", "informativeText", "numeric"))
      .fit(trainingDataset)

    val scoringDataset = spark.createDataFrame(Seq(
      (4, 5.0, null, "yellow bird"),
      (5, 6.0, "", "red fox"),
      (6, 7.0, "novel scoring text", "blue dog")))
      .toDF(mockLabelColumn, "numeric", "constantText", "informativeText")

    val baseResult = baseModel.transform(scoringDataset).select(featuresColumn)
    val resultWithConstantText = modelWithConstantText.transform(scoringDataset).select(featuresColumn)

    assert(baseResult.collect().map(_.getAs[Vector](0)).toSeq ===
      resultWithConstantText.collect().map(_.getAs[Vector](0)).toSeq)
    assert(baseResult.schema(featuresColumn).metadata === resultWithConstantText.schema(featuresColumn).metadata)
    assert(AttributeGroup.fromStructField(baseResult.schema(featuresColumn)).numAttributes ===
      AttributeGroup.fromStructField(resultWithConstantText.schema(featuresColumn)).numAttributes)

    val modelDir = new File(tmpDir.toFile, "issue1667-featurize-model")
    modelWithConstantText.write.overwrite().save(modelDir.toString)
    val loadedModel = PipelineModel.load(modelDir.toString)
    val loadedResult = loadedModel.transform(scoringDataset).select(featuresColumn)
    assert(verifyResult(resultWithConstantText, loadedResult))
  }

  test("issue 1667 - collapsed text coexists with vectorAssemblerHandleInvalid keep and defaultCopy") {
    val trainingDataset = spark.createDataFrame(Seq(
      (0, "always the same", 1.0),
      (1, "always the same", 2.0),
      (2, "always the same", 3.0)))
      .toDF(mockLabelColumn, "constantText", "numeric")
    val scoringDataset = spark.createDataFrame(Seq[(Int, String, JDouble)](
      (0, null, null),
      (1, "", 4.0),
      (2, "novel scoring text", Double.NaN)))
      .toDF(mockLabelColumn, "constantText", "numeric")

    val featurize = new Featurize()
      .setNumFeatures(1024)
      .setOutputCol(featuresColumn)
      .setInputCols(Array("constantText", "numeric"))
      .setImputeMissing(false)
      .setVectorAssemblerHandleInvalid("keep")
    val copied = featurize.copy(new ParamMap()).asInstanceOf[Featurize]

    assert(copied.getVectorAssemblerHandleInvalid == "keep")
    val result = copied.fit(trainingDataset).transform(scoringDataset)
    val byLabel = result.select(mockLabelColumn, featuresColumn).collect().map { row =>
      row.getAs[Int](mockLabelColumn) -> row.getAs[Vector](featuresColumn)
    }.toMap

    assert(byLabel.values.forall(_.size == 1))
    assert(byLabel(0)(0).isNaN)
    assert(byLabel(1)(0) == 4.0)
    assert(byLabel(2)(0).isNaN)
    val attributes = AttributeGroup.fromStructField(result.schema(featuresColumn))
    assert(attributes.size == 1)
    assert(attributes.attributes.get.flatMap(_.name).toSeq == Seq("numeric"))
  }

  test("issue 1667 - featurize fails clearly when all text features are constant") {
    val dataset = spark.createDataFrame(Seq(Tuple1("2"), Tuple1("2"), Tuple1("2"))).toDF("text")

    val ex = intercept[IllegalArgumentException] {
      new Featurize()
        .setNumFeatures(1024)
        .setOutputCol(featuresColumn)
        .setInputCols(Array("text"))
        .fit(dataset)
    }

    assert(ex.getMessage.toLowerCase.contains("no usable"))
    assert(ex.getMessage.contains("text"))
  }

  test("issue 1667 - featurize fails clearly when text vocabulary is empty or null") {
    val dataset = spark.createDataFrame(Seq[(Int, String)](
      (0, null),
      (1, ""),
      (2, null)))
      .toDF(mockLabelColumn, "emptyText")

    val ex = intercept[IllegalArgumentException] {
      new Featurize()
        .setNumFeatures(1024)
        .setOutputCol(featuresColumn)
        .setInputCols(Array("emptyText"))
        .fit(dataset)
    }

    assert(ex.getMessage.toLowerCase.contains("no usable"))
    assert(ex.getMessage.contains("'emptyText'"))
    assert(ex.getMessage.contains("constant, empty"))
  }

  test("issue 1667 - constant numeric columns keep their original semantics") {
    val dataset = spark.createDataFrame(Seq(
      (0, 5.0, 1.0),
      (1, 5.0, 2.0),
      (0, 5.0, 3.0)))
      .toDF(mockLabelColumn, "constantNum", "varyingNum")

    val result = featurize(dataset).select(featuresColumn).collect().map(_.getAs[Vector](0)).toSeq
    assert(result.forall(_.size == 2))
    assert(result.map(_.toArray.sorted.toSeq) === Seq(
      Seq(1.0, 5.0),
      Seq(2.0, 5.0),
      Seq(3.0, 5.0)))
  }

  test("issue 1667 - constant categorical columns keep Spark one-hot validation") {
    val dataset = spark.createDataFrame(Seq(
      (0, "cat"),
      (1, "cat"),
      (0, "cat")))
      .toDF(mockLabelColumn, "animal")

    val indexed = new ValueIndexer().setInputCol("animal").setOutputCol("animal").fit(dataset).transform(dataset)

    val ex = intercept[IllegalArgumentException] {
      new Featurize()
        .setOutputCol(featuresColumn)
        .setInputCols(Array("animal"))
        .setOneHotEncodeCategoricals(true)
        .fit(indexed)
    }

    assert(ex.getMessage.contains("animal"))
    assert(ex.getMessage.contains("at least two distinct values"))
  }

  // This test currently fails on ValueIndexer, where we should handle missing values (unlike spark,
  // which fails with a null reference exception)
  ignore("Featurizing with categorical columns that have missings - using one hot encoding") {
    val cat = "Cat"
    val dog = "Dog"
    val bird = "Bird"
    val dataset: DataFrame = spark.createDataFrame(Seq(
      (0, cat),
      (1, null),
      (0, bird),
      (1, null),
      (0, bird),
      (1, dog)))
      .toDF(mockLabelColumn, "col1")

    val model1 = new ValueIndexer().setInputCol("col1").setOutputCol("col1").fit(dataset)
    val catDataset = model1.transform(dataset)

    val result: DataFrame = featurizeAndVerifyResult(catDataset, historicOneHotMissingsFile,
      oneHotEncode = true)
    // Verify that features column has the correct number of slots
    assert(result.first().getAs[DenseVector](featuresColumn).size == 4)

    // Verify without one-hot encoding we get expected data
    val resultNoOneHot: DataFrame = featurizeAndVerifyResult(catDataset, historicNoOneHotMissingsFile)
    // Verify that features column has the correct number of slots
    assert(resultNoOneHot.first().getAs[DenseVector](featuresColumn).size == 4)
  }

  val missingValueLabelColumn = "missingLabel"
  val missingValueInputCol = "missingCol"

  lazy val missingValueDataset: DataFrame = spark.createDataFrame(Seq[(Int, JDouble)](
    (0, 1.0),
    (1, null),
    (2, Double.NaN),
    (3, 4.0)))
    .toDF(missingValueLabelColumn, missingValueInputCol)

  private def missingValueFeaturize: Featurize =
    new Featurize()
      .setInputCols(Array(missingValueInputCol))
      .setOutputCol(featuresColumn)
      .setImputeMissing(false)

  private def firstSlotByLabel(result: DataFrame): Map[Int, Double] = {
    result.select(missingValueLabelColumn, featuresColumn).collect().map { row =>
      row.getAs[Int](missingValueLabelColumn) -> row.getAs[Vector](featuresColumn)(0)
    }.toMap
  }

  test("Featurize vectorAssemblerHandleInvalid defaults to skip and preserves prior row-dropping behavior") {
    val feat = missingValueFeaturize
    assert(feat.getVectorAssemblerHandleInvalid == "skip")
    val result = feat.fit(missingValueDataset).transform(missingValueDataset)
    assert(result.count() == 2)
    val byLabel = firstSlotByLabel(result)
    assert(byLabel.keySet == Set(0, 3))
    assert(byLabel(0) == 1.0)
    assert(byLabel(3) == 4.0)
  }

  test("Featurize vectorAssemblerHandleInvalid=keep preserves all rows and encodes missing values as NaN") {
    val feat = missingValueFeaturize.setVectorAssemblerHandleInvalid("keep")
    val result = feat.fit(missingValueDataset).transform(missingValueDataset)
    assert(result.count() == 4)
    val byLabel = firstSlotByLabel(result)
    assert(byLabel(0) == 1.0)
    assert(byLabel(1).isNaN)
    assert(byLabel(2).isNaN)
    assert(byLabel(3) == 4.0)
  }

  test("Featurize vectorAssemblerHandleInvalid=error throws on missing scalar values") {
    val feat = missingValueFeaturize.setVectorAssemblerHandleInvalid("error")
    assertSparkException[SparkException](feat, missingValueDataset)
  }

  test("Featurize setVectorAssemblerHandleInvalid rejects unsupported values") {
    val ex = intercept[IllegalArgumentException] {
      missingValueFeaturize.setVectorAssemblerHandleInvalid("bogus")
    }
    assert(ex.getMessage.contains("vectorAssemblerHandleInvalid"))
  }

  test("Featurize copy retains vectorAssemblerHandleInvalid and other configured params") {
    val feat = missingValueFeaturize.setVectorAssemblerHandleInvalid("keep")
    val copied = feat.copy(new ParamMap()).asInstanceOf[Featurize]
    assert(copied.uid == feat.uid)
    assert(copied.getVectorAssemblerHandleInvalid == "keep")
    assert(!copied.getImputeMissing)
    assert(copied.getInputCols.toSeq == feat.getInputCols.toSeq)
    assert(copied.getOutputCol == feat.getOutputCol)
    val result = copied.fit(missingValueDataset).transform(missingValueDataset)
    assert(result.count() == 4)

    val overridden = feat.copy(new ParamMap()
      .put(feat.vectorAssemblerHandleInvalid, "error")
      .put(feat.imputeMissing, true))
      .asInstanceOf[Featurize]
    assert(overridden.getVectorAssemblerHandleInvalid == "error")
    assert(overridden.getImputeMissing)
  }

  test("Featurize keep supports sparse, dense, and null vectors and preserves feature metadata") {
    val vectorInputCol = "vectorInput"
    val scalarInputCol = "scalarInput"
    val rawDataset = spark.createDataFrame(Seq[(Int, Vector, JDouble)](
      (0, Vectors.sparse(3, Seq(0 -> 1.0)), 0.0),
      (1, Vectors.dense(0.0, Double.NaN, 2.0), 3.0),
      (2, null, 4.0)))
      .toDF(missingValueLabelColumn, vectorInputCol, scalarInputCol)
    val dataset = new VectorSizeHint()
      .setInputCol(vectorInputCol)
      .setSize(3)
      .setHandleInvalid("optimistic")
      .transform(rawDataset)

    val feat = new Featurize()
      .setInputCols(Array(vectorInputCol, scalarInputCol))
      .setOutputCol(featuresColumn)
      .setImputeMissing(false)
      .setVectorAssemblerHandleInvalid("keep")
    val result = feat.fit(dataset).transform(dataset)
    assert(result.count() == 3)

    val attributeGroup = AttributeGroup.fromStructField(result.schema(featuresColumn))
    val attributes = attributeGroup.attributes.get
    assert(attributeGroup.size == 4)
    assert(attributes.flatMap(_.name).toSet ==
      Set(s"${vectorInputCol}_0", s"${vectorInputCol}_1", s"${vectorInputCol}_2", scalarInputCol))

    val byLabel = result.select(missingValueLabelColumn, featuresColumn).collect().map { row =>
      val namedValues = attributes.zip(row.getAs[Vector](featuresColumn).toArray).map {
        case (attribute, value) => attribute.name.get -> value
      }.toMap
      row.getAs[Int](missingValueLabelColumn) -> (row.getAs[Vector](featuresColumn), namedValues)
    }.toMap

    assert(byLabel(0)._1.isInstanceOf[SparseVector])
    assert(byLabel(0)._2(s"${vectorInputCol}_0") == 1.0)
    assert(byLabel(0)._2(s"${vectorInputCol}_1") == 0.0)
    assert(byLabel(0)._2(s"${vectorInputCol}_2") == 0.0)
    assert(byLabel(0)._2(scalarInputCol) == 0.0)

    assert(byLabel(1)._1.isInstanceOf[DenseVector])
    assert(byLabel(1)._2(s"${vectorInputCol}_0") == 0.0)
    assert(byLabel(1)._2(s"${vectorInputCol}_1").isNaN)
    assert(byLabel(1)._2(s"${vectorInputCol}_2") == 2.0)
    assert(byLabel(1)._2(scalarInputCol) == 3.0)

    assert(byLabel(2)._2(s"${vectorInputCol}_0").isNaN)
    assert(byLabel(2)._2(s"${vectorInputCol}_1").isNaN)
    assert(byLabel(2)._2(s"${vectorInputCol}_2").isNaN)
    assert(byLabel(2)._2(scalarInputCol) == 4.0)
  }

  test("Featurize estimator save/load retains vectorAssemblerHandleInvalid") {
    val feat = missingValueFeaturize.setVectorAssemblerHandleInvalid("keep")
    val path = new File(tmpDir.toFile, "featurize-estimator-keep").toString
    feat.write.overwrite().save(path)
    val loaded = Featurize.load(path)
    assert(loaded.getVectorAssemblerHandleInvalid == "keep")
    val result = loaded.fit(missingValueDataset).transform(missingValueDataset)
    assert(result.count() == 4)
    val byLabel = firstSlotByLabel(result)
    assert(byLabel(1).isNaN)
    assert(byLabel(2).isNaN)
  }

  test("Featurize keep works in a persisted Pipeline with a downstream transformer") {
    val feat = missingValueFeaturize.setVectorAssemblerHandleInvalid("keep")
    val selectedFeaturesColumn = "selectedFeatures"
    val pipeline = new Pipeline().setStages(Array(
      feat,
      new VectorSlicer()
        .setInputCol(featuresColumn)
        .setOutputCol(selectedFeaturesColumn)
        .setIndices(Array(0))))
    val model = pipeline.fit(missingValueDataset)
    val path = new File(tmpDir.toFile, "featurize-pipeline-model-keep").toString
    model.write.overwrite().save(path)
    val loadedModel = PipelineModel.load(path)
    val result = loadedModel.transform(missingValueDataset)
    assert(result.count() == 4)
    val byLabel = firstSlotByLabel(result)
    assert(byLabel(0) == 1.0)
    assert(byLabel(1).isNaN)
    assert(byLabel(2).isNaN)
    assert(byLabel(3) == 4.0)
    assert(result.select(selectedFeaturesColumn).collect().forall {
      _.getAs[Vector](selectedFeaturesColumn).size == 1
    })
  }

  private def featurize(dataset: DataFrame,
                        oneHotEncode: Boolean = false,
                        includeFeaturesColumns: Boolean = true): DataFrame = {
    val featureColumns = dataset.columns.filter(_ != mockLabelColumn)
    val feat = new Featurize()
      .setNumFeatures(10)
      .setOutputCol(featuresColumn)
      .setInputCols(featureColumns)
      .setOneHotEncodeCategoricals(oneHotEncode)
    val featModel = feat.fit(dataset)
    val result = featModel.transform(dataset)
    if (includeFeaturesColumns) result else result.select(featuresColumn)
  }

  private def featurizeAndVerifyResult(dataset: DataFrame,
                                       historicFile: File,
                                       oneHotEncode: Boolean = false,
                                       includeFeaturesColumns: Boolean = true): DataFrame = {
    val result = featurize(dataset, oneHotEncode, includeFeaturesColumns)
    if (!Files.exists(historicFile.toPath)) {
      // Store result in file for future
      val directory = historicFile.toString.replace(".json", "")
      result.repartition(1).write.mode("overwrite").json(directory)
      val directoryFile = new File(directory)
      val jsonFile = directoryFile.listFiles().filter(file => file.toString.endsWith(".json"))(0)
      jsonFile.renameTo(historicFile)
      FileUtils.forceDelete(directoryFile)
    }
    val expResult = spark.read.json(historicFile.toString)
    // Verify the results are the same
    verifyResult(expResult, result)
    result
  }

  override def testObjects(): List[TestObject[Featurize]] = List(new TestObject(
    new Featurize().setInputCols(mockDataset.columns).setOutputCol(featuresColumn), mockDataset))

  override def reader: MLReadable[_] = Featurize

  override def modelReader: MLReadable[_] = PipelineModel
}
