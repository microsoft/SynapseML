// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{EstimatorFuzzing, TestObject, TransformerFuzzing}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.MLReadable
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row}

import java.io.File
import scala.collection.JavaConverters._

/** Shared input fixture and projection helper used by both the estimator suite
  * [[LumpFeaturesSuite]] and the model suite [[LumpFeaturesModelSuite]].
  */
trait LumpFeaturesTestData extends TestBase {

  import spark.implicits._

  protected lazy val df: DataFrame = Seq(
    ("apple", "red"),
    ("apple", "red"),
    ("apple", "blue"),
    ("banana", "red"),
    ("cherry", "green")
  ).toDF("f1", "f2")

  protected val other: String = "__other__"

  protected def dump(data: DataFrame, cols: Seq[String]): List[String] = {
    val projected = data.select(cols.map(col): _*)
    projected.collect().map { r =>
      cols.indices.map(i => if (r.isNullAt(i)) "<null>" else r.get(i).toString).mkString("|")
    }.sorted.toList
  }
}

//scalastyle:off null
class LumpFeaturesSuite extends EstimatorFuzzing[LumpFeatures] with LumpFeaturesTestData {

  import spark.implicits._

  test("multi-column top-K retains learned values and lumps the rest") {
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 1, "f2" -> 1)).fit(df)
    assert(model.getKeptValues("f1") == Seq("apple"))
    assert(model.getKeptValues("f2") == Seq("red"))
    val out = model.transform(df)
    assert(out.columns.toSeq == Seq("f1", "f2"))
    assert(dump(out, Seq("f1", "f2")) == List(
      s"$other|$other", s"$other|red", s"apple|$other", "apple|red", "apple|red"))
  }

  test("stable ranking is count desc then value asc for ties") {
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 2)).fit(df)
    assert(model.getKeptValues("f1") == Seq("apple", "banana"))
    assert(dump(model.transform(df), Seq("f1")) == List(other, "apple", "apple", "apple", "banana"))
  }

  test("rare and unseen non-null values map to the other bucket") {
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 1)).fit(df)
    val scoring = Seq(("banana", "red"), ("durian", "red"), ("apple", "red")).toDF("f1", "f2")
    assert(dump(model.transform(scoring), Seq("f1")) == List(other, other, "apple"))
  }

  test("handleNull keep preserves null; handleNull other maps null to otherValue") {
    val dfN = Seq(("apple", "red"), ("apple", "red"), (null, "red"), ("banana", "red"))
      .toDF("f1", "f2")
    val keepModel = new LumpFeatures().setLumpRules(Map("f1" -> 1)).setHandleNull("keep").fit(dfN)
    assert(dump(keepModel.transform(dfN), Seq("f1")) == List("<null>", other, "apple", "apple"))
    val otherModel = new LumpFeatures().setLumpRules(Map("f1" -> 1)).setHandleNull("other").fit(dfN)
    assert(dump(otherModel.transform(dfN), Seq("f1")) == List(other, other, "apple", "apple"))
  }

  test("all-null column yields empty learned levels and maps values accordingly") {
    val dfAllNull = Seq((null.asInstanceOf[String], "red"), (null.asInstanceOf[String], "blue"))
      .toDF("f1", "f2")
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 2)).setHandleNull("keep").fit(dfAllNull)
    assert(model.getKeptValues("f1").isEmpty)
    assert(dump(model.transform(dfAllNull), Seq("f1")) == List("<null>", "<null>"))
    val scoring = Seq(("zebra", "red")).toDF("f1", "f2")
    assert(dump(model.transform(scoring), Seq("f1")) == List(other))
  }

  test("empty learned levels via manually constructed model maps all non-null to otherValue") {
    val model = new LumpFeaturesModel()
      .setLumpRules(Map("f1" -> 2))
      .setKeptValuesJson(Map("f1" -> "[]"))
      .setHandleNull("keep")
    assert(dump(model.transform(df), Seq("f1")) == List(other, other, other, other, other))
  }

  test("fit rejects otherValue collision even when the value is rare") {
    val dfCollide = Seq(("apple", "red"), ("apple", "red"), (other, "blue")).toDF("f1", "f2")
    val ex = intercept[IllegalArgumentException] {
      new LumpFeatures().setLumpRules(Map("f1" -> 1)).fit(dfCollide)
    }
    assert(ex.getMessage.toLowerCase.contains("othervalue"))
  }

  test("model transform rejects changed otherValue that collides with a retained value") {
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 1)).fit(df)
    model.setOtherValue("apple")
    intercept[IllegalArgumentException] {
      model.transform(df).collect()
    }
  }

  test("fit validates rules, K, column names, presence and string type") {
    intercept[IllegalArgumentException] {
      new LumpFeatures().setLumpRules(Map.empty[String, Int]).fit(df)
    }
    intercept[IllegalArgumentException] {
      new LumpFeatures().fit(df)
    }
    intercept[IllegalArgumentException] {
      new LumpFeatures().setLumpRules(Map("f1" -> 0)).fit(df)
    }
    intercept[IllegalArgumentException] {
      new LumpFeatures().setLumpRules(Map("f1" -> -3)).fit(df)
    }
    intercept[IllegalArgumentException] {
      new LumpFeatures().setLumpRules(Map("" -> 1)).fit(df)
    }
    intercept[IllegalArgumentException] {
      new LumpFeatures().setLumpRules(Map("missing" -> 1)).fit(df)
    }
    val dfInt = Seq(("apple", 1), ("banana", 2)).toDF("f1", "n")
    intercept[IllegalArgumentException] {
      new LumpFeatures().setLumpRules(Map("n" -> 1)).fit(dfInt)
    }
  }

  test("model validates state and rule consistency") {
    val mismatch = new LumpFeaturesModel()
      .setLumpRules(Map("f1" -> 1))
      .setKeptValuesJson(Map("f2" -> "[\"red\"]"))
    intercept[IllegalArgumentException] { mismatch.transform(df).collect() }
    val tooMany = new LumpFeaturesModel()
      .setLumpRules(Map("f1" -> 1))
      .setKeptValuesJson(Map("f1" -> "[\"apple\",\"banana\"]"))
    intercept[IllegalArgumentException] { tooMany.transform(df).collect() }
  }

  test("schema matches transform exactly with cleared metadata and correct nullability") {
    val schema = StructType(Seq(
      StructField("f1", StringType, nullable = true),
      StructField("f2", StringType, nullable = true),
      StructField("keep_me", IntegerType, nullable = false)))
    val rows = Seq(Row("apple", "red", 1), Row("banana", "red", 2), Row(null, "blue", 3))
    val md = new MetadataBuilder().putString("foo", "bar").build()
    val base = spark.createDataFrame(rows.asJava, schema)
    val dfMeta = base.withColumn("f1", col("f1").as("f1", md))

    val keepModel = new LumpFeatures().setLumpRules(Map("f1" -> 1)).setHandleNull("keep").fit(dfMeta)
    val declaredKeep = keepModel.transformSchema(dfMeta.schema)
    assert(declaredKeep == keepModel.transform(dfMeta).schema)
    assert(declaredKeep.fieldNames.toSeq == Seq("f1", "f2", "keep_me"))
    assert(declaredKeep("f1").dataType == StringType)
    assert(declaredKeep("f1").nullable)
    assert(declaredKeep("f1").metadata == Metadata.empty)
    assert(declaredKeep("f2") == dfMeta.schema("f2"))
    assert(declaredKeep("keep_me") == dfMeta.schema("keep_me"))
    assert(keepModel.transform(dfMeta).schema("f1").metadata == Metadata.empty)

    val otherModel = new LumpFeatures().setLumpRules(Map("f1" -> 1)).setHandleNull("other").fit(dfMeta)
    val declaredOther = otherModel.transformSchema(dfMeta.schema)
    assert(declaredOther == otherModel.transform(dfMeta).schema)
    assert(!declaredOther("f1").nullable)
  }

  test("estimator and model copy preserve parameters and learned state") {
    val est = new LumpFeatures().setLumpRules(Map("f1" -> 1)).setOtherValue("X").setHandleNull("other")
    val estCopy = est.copy(new ParamMap())
    assert(estCopy.getLumpRules == Map("f1" -> 1))
    assert(estCopy.getOtherValue == "X")
    assert(estCopy.getHandleNull == "other")
    val model = est.fit(df)
    val modelCopy = model.copy(new ParamMap())
    assert(modelCopy.getKeptValues == model.getKeptValues)
    assert(modelCopy.getOtherValue == "X")
    assert(modelCopy.getHandleNull == "other")
    assert(modelCopy.getLumpRules == Map("f1" -> 1))
  }

  test("legacy JSON-string and Java HashMap setters populate lumpRules") {
    val fromJson = new LumpFeatures().setLumpRules("{\"f1\":2,\"f2\":1}")
    assert(fromJson.getLumpRules == Map("f1" -> 2, "f2" -> 1))
    val jmap = new java.util.HashMap[String, Int]()
    jmap.put("f1", 3)
    val fromJava = new LumpFeatures().setLumpRules(jmap)
    assert(fromJava.getLumpRules == Map("f1" -> 3))
  }

  test("persisted learned values survive arbitrary strings round-trip") {
    val weird = Seq(
      ("a\"b", "x"),
      ("[bracket]", "x"),
      ("\u00fcn\u00eecod\u00e9", "x"),
      ("with,comma", "x"),
      ("back\\slash", "x"),
      ("a\"b", "y")
    ).toDF("f1", "f2")
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 10)).fit(weird)
    val path = new File(tmpDir.toFile, "lump-weird").toString
    model.write.overwrite().save(path)
    val loaded = LumpFeaturesModel.load(path)
    assert(loaded.getKeptValues("f1").toSet ==
      Set("a\"b", "[bracket]", "\u00fcn\u00eecod\u00e9", "with,comma", "back\\slash"))
    assertDFEq(model.transform(weird), loaded.transform(weird))
  }

  private def bq(name: String): Column = col("`" + name.replace("`", "``") + "`")

  private def dumpQ(data: DataFrame, cols: Seq[String]): List[String] = {
    val projected = data.select(cols.map(bq): _*)
    projected.collect().map { r =>
      cols.indices.map(i => if (r.isNullAt(i)) "<null>" else r.get(i).toString).mkString("|")
    }.sorted.toList
  }

  test("rule columns named count, dotted, and embedded-backtick fit and transform correctly") {
    val specialDf = Seq(
      ("apple", "red", "x"),
      ("apple", "red", "x"),
      ("apple", "blue", "y"),
      ("banana", "red", "x")
    ).toDF("count", "a.b", "a`b")
    val rules = Map("count" -> 1, "a.b" -> 1, "a`b" -> 1)
    val model = new LumpFeatures().setLumpRules(rules).fit(specialDf)
    assert(model.getKeptValues("count") == Seq("apple"))
    assert(model.getKeptValues("a.b") == Seq("red"))
    assert(model.getKeptValues("a`b") == Seq("x"))
    val out = model.transform(specialDf)
    assert(out.columns.toSeq == Seq("count", "a.b", "a`b"))
    assert(dumpQ(out, Seq("count")) == List(other, "apple", "apple", "apple"))
    assert(dumpQ(out, Seq("a.b")) == List(other, "red", "red", "red"))
    assert(dumpQ(out, Seq("a`b")) == List(other, "x", "x", "x"))
    val collideDf = Seq(("apple", "red", "x"), (other, "blue", "y")).toDF("count", "a.b", "a`b")
    val ex = intercept[IllegalArgumentException] {
      new LumpFeatures().setLumpRules(Map("count" -> 1)).fit(collideDf)
    }
    assert(ex.getMessage.toLowerCase.contains("othervalue"))
  }
  override def testObjects(): Seq[TestObject[LumpFeatures]] = Seq(
    new TestObject(new LumpFeatures().setLumpRules(Map("f1" -> 1, "f2" -> 1)), df))

  override def reader: MLReadable[_] = LumpFeatures

  override def modelReader: MLReadable[_] = LumpFeaturesModel

}

/** Dedicated fuzzing suite for [[LumpFeaturesModel]]. LumpFeatures serializes fitted models, so the
  * model needs its own Experiment/Serialization/Python/R fuzzer coverage. The estimator suite only
  * registers a fuzzer for the estimator type, which left the model without any discovered fuzzer.
  * The test object is a valid, manually constructed persisted model (learned levels supplied directly)
  * paired with a deterministic input DataFrame.
  */
class LumpFeaturesModelSuite extends TransformerFuzzing[LumpFeaturesModel] with LumpFeaturesTestData {

  private def persistedModel: LumpFeaturesModel = new LumpFeaturesModel()
    .setLumpRules(Map("f1" -> 1, "f2" -> 1))
    .setKeptValuesJson(Map("f1" -> "[\"apple\"]", "f2" -> "[\"red\"]"))
    .setOtherValue(other)
    .setHandleNull("keep")

  test("manually constructed model retains learned values and lumps the rest deterministically") {
    val out = persistedModel.transform(df)
    assert(out.columns.toSeq == Seq("f1", "f2"))
    assert(dump(out, Seq("f1", "f2")) == List(
      s"$other|$other", s"$other|red", s"apple|$other", "apple|red", "apple|red"))
  }

  override def testObjects(): Seq[TestObject[LumpFeaturesModel]] =
    Seq(new TestObject(persistedModel, df))

  override def reader: MLReadable[_] = LumpFeaturesModel

}
