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
import spray.json._

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
      .setKeptValuesJson(Map("f1" -> "{\"topK\":2,\"values\":[]}"))
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

  test("fitted model rejects direct otherValue mutation to an observed non-retained category") {
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 1)).fit(df)
    assert(model.getKeptValues("f1") == Seq("apple"))
    val ex = intercept[IllegalArgumentException] { model.setOtherValue("banana") }
    assert(ex.getMessage.toLowerCase.contains("othervalue"))
    assert(model.getOtherValue == other)
    assert(dump(model.transform(df), Seq("f1")) == List(other, other, "apple", "apple", "apple"))
  }

  test("generic otherValue Param mutation is rejected and leaves the fitted model unchanged") {
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 1)).fit(df)
    val ex = intercept[IllegalArgumentException] {
      model.set(model.otherValue, "banana")
    }
    assert(ex.getMessage.toLowerCase.contains("othervalue"))
    assert(model.getOtherValue == other)
    assert(dump(model.transform(df), Seq("f1")) == List(other, other, "apple", "apple", "apple"))
  }

  test("generic clear and mutation cannot remove or corrupt fitted learned state") {
    val model = new LumpFeatures()
      .setLumpRules(Map("f1" -> 1))
      .setOtherValue("fallback")
      .fit(df)
    val learned = model.getKeptValuesJson

    intercept[IllegalArgumentException] { model.clear(model.getParam("keptValuesJson")) }
    assert(model.getKeptValuesJson == learned)

    intercept[IllegalArgumentException] { model.set(model.keptValuesJson, Map.empty[String, String]) }
    assert(model.getKeptValuesJson == learned)

    intercept[IllegalArgumentException] { model.clear(model.otherValue) }
    assert(model.getOtherValue == "fallback")
    intercept[IllegalArgumentException] { model.setOtherValue("banana") }
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
      .setKeptValuesJson(Map("f2" -> "{\"topK\":1,\"values\":[\"red\"]}"))
    intercept[IllegalArgumentException] { mismatch.transform(df).collect() }
    val tooMany = new LumpFeaturesModel()
      .setLumpRules(Map("f1" -> 1))
      .setKeptValuesJson(Map("f1" -> "{\"topK\":1,\"values\":[\"apple\",\"banana\"]}"))
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

  test("fitted model rejects increasing or decreasing K even when distinct values are fewer than K") {
    val twoDistinct = Seq(("apple", "x"), ("apple", "x"), ("banana", "x")).toDF("f1", "f2")
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 3)).fit(twoDistinct)
    assert(model.getKeptValues("f1") == Seq("apple", "banana"))
    intercept[IllegalArgumentException] { model.setLumpRules(Map("f1" -> 5)) }
    intercept[IllegalArgumentException] { model.setLumpRules(Map("f1" -> 2)) }
    assert(model.getLumpRules == Map("f1" -> 3))
    assert(model.getKeptValues("f1") == Seq("apple", "banana"))
  }

  test("setting the exact fitted lumpRules map on a fitted model is an allowed no-op") {
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 1, "f2" -> 1)).fit(df)
    val before = dump(model.transform(df), Seq("f1", "f2"))
    val returned = model.setLumpRules(Map("f1" -> 1, "f2" -> 1))
    assert(returned.getLumpRules == Map("f1" -> 1, "f2" -> 1))
    assert(dump(model.transform(df), Seq("f1", "f2")) == before)
  }

  test("loaded model and its copy retain immutable fitted rules and reject incompatible changes") {
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 1, "f2" -> 1)).fit(df)
    val path = new File(tmpDir.toFile, "lump-immutable").toString
    model.write.overwrite().save(path)
    val loaded = LumpFeaturesModel.load(path)
    assert(loaded.getLumpRules == Map("f1" -> 1, "f2" -> 1))
    assert(loaded.getKeptValues == model.getKeptValues)
    assertDFEq(loaded.transform(df), model.transform(df))
    intercept[IllegalArgumentException] { loaded.setLumpRules(Map("f1" -> 2, "f2" -> 1)) }
    val copied = loaded.copy(new ParamMap())
    assert(copied.getKeptValues == model.getKeptValues)
    assert(copied.getLumpRules == Map("f1" -> 1, "f2" -> 1))
    intercept[IllegalArgumentException] { copied.setLumpRules(Map("f1" -> 3, "f2" -> 1)) }
  }

  test("copy with an incompatible lumpRules ParamMap override is rejected") {
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 1)).fit(df)
    val incompatible = new ParamMap().put(model.lumpRules, Map("f1" -> 5))
    intercept[IllegalArgumentException] { model.copy(incompatible) }
    val same = new ParamMap().put(model.lumpRules, Map("f1" -> 1))
    val copied = model.copy(same)
    assert(copied.getLumpRules == Map("f1" -> 1))
    assert(copied.getKeptValues == model.getKeptValues)
  }

  test("copy rejects forged learned state, otherValue, and rules atomically but accepts identical state") {
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 1)).fit(df)
    val learned = model.getKeptValuesJson
    val before = dump(model.transform(df), Seq("f1"))

    val incompatibleOther = new ParamMap().put(model.otherValue, "banana")
    intercept[IllegalArgumentException] { model.copy(incompatibleOther) }

    val incompatibleRules = new ParamMap().put(model.lumpRules, Map("f1" -> 2))
    intercept[IllegalArgumentException] { model.copy(incompatibleRules) }

    val forged = learned.updated("f1", LumpFeaturesModel.encodeKept(1, Seq("banana"), other))
    val forgedState = new ParamMap().put(model.keptValuesJson, forged)
    intercept[IllegalArgumentException] { model.copy(forgedState) }

    val missingState = new ParamMap().put(model.keptValuesJson, Map.empty[String, String])
    intercept[IllegalArgumentException] { model.copy(missingState) }

    val identicalState = new ParamMap().put(model.keptValuesJson, learned)
    val copied = model.copy(identicalState)
    assert(copied.getKeptValuesJson == learned)
    assertDFEq(copied.transform(df), model.transform(df))

    assert(model.getOtherValue == other)
    assert(model.getLumpRules == Map("f1" -> 1))
    assert(model.getKeptValuesJson == learned)
    assert(dump(model.transform(df), Seq("f1")) == before)
  }

  test("model-state validation rejects lumpRules mutated through the generic Param path") {
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 1)).fit(df)
    model.set(model.lumpRules, Map("f1" -> 5))
    val ex = intercept[IllegalArgumentException] { model.transform(df).collect() }
    assert(ex.getMessage.toLowerCase.contains("lumprules") || ex.getMessage.toLowerCase.contains("top-k"))
  }

  test("fitted model setLumpRules overloads (Scala Map, Java HashMap, JSON string) all reject changes") {
    def fitted(): LumpFeaturesModel = new LumpFeatures().setLumpRules(Map("f1" -> 1)).fit(df)
    intercept[IllegalArgumentException] { fitted().setLumpRules(Map("f1" -> 2)) }
    val jmap = new java.util.HashMap[String, Int]()
    jmap.put("f1", 2)
    intercept[IllegalArgumentException] { fitted().setLumpRules(jmap) }
    intercept[IllegalArgumentException] { fitted().setLumpRules("{\"f1\":2}") }
  }

  test("minCount lumps values below the count threshold before the top-K cap applies") {
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 3)).setMinCount(2).fit(df)
    assert(model.getKeptValues("f1") == Seq("apple"))
    assert(dump(model.transform(df), Seq("f1")) == List(other, other, "apple", "apple", "apple"))
  }

  test("minFreq lumps values below the frequency share before the top-K cap applies") {
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 3)).setMinFreq(0.25).fit(df)
    assert(model.getKeptValues("f1") == Seq("apple"))
    val permissive = new LumpFeatures().setLumpRules(Map("f1" -> 3)).setMinFreq(0.15).fit(df)
    assert(permissive.getKeptValues("f1") == Seq("apple", "banana", "cherry"))
  }

  test("top-K still caps values that clear the frequency filters") {
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 1)).setMinCount(1).setMinFreq(0.1).fit(df)
    assert(model.getKeptValues("f1") == Seq("apple"))
  }

  test("frequency filters are per column and are computed against non-null rows only") {
    val dfN = Seq(("apple", "red"), ("apple", "red"), (null, "red"), ("banana", "blue"))
      .toDF("f1", "f2")
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 3, "f2" -> 3)).setMinFreq(0.5).fit(dfN)
    // f1 has 3 non-null rows, so apple (2/3) clears 0.5 and banana (1/3) does not.
    assert(model.getKeptValues("f1") == Seq("apple"))
    // f2 has 4 non-null rows, so red (3/4) clears 0.5 and blue (1/4) does not.
    assert(model.getKeptValues("f2") == Seq("red"))
  }

  test("minCount and minFreq default to no-ops and reject out-of-range values") {
    val est = new LumpFeatures().setLumpRules(Map("f1" -> 3))
    assert(est.getMinCount == 1)
    assert(est.getMinFreq == 0.0)
    intercept[IllegalArgumentException] { est.setMinCount(0) }
    intercept[IllegalArgumentException] { est.setMinFreq(-0.1) }
    intercept[IllegalArgumentException] { est.setMinFreq(1.1) }
    assert(est.fit(df).getKeptValues("f1") == Seq("apple", "banana", "cherry"))
  }

  test("multi-column fit agrees with fitting each column on its own") {
    val rules = Map("f1" -> 2, "f2" -> 2)
    val together = new LumpFeatures().setLumpRules(rules).fit(df).getKeptValues
    rules.foreach { case (name, k) =>
      val alone = new LumpFeatures().setLumpRules(Map(name -> k)).fit(df).getKeptValues(name)
      assert(together(name) == alone, s"column $name disagreed between joint and single-column fits")
    }
  }

  test("outputCols appends lumped columns and leaves the raw columns untouched") {
    val model = new LumpFeatures()
      .setLumpRules(Map("f1" -> 1))
      .setOutputCols(Map("f1" -> "f1_lumped"))
      .fit(df)
    val out = model.transform(df)
    assert(out.columns.toSeq == Seq("f1", "f2", "f1_lumped"))
    assert(dump(out, Seq("f1", "f1_lumped")) == List(
      "apple|apple", "apple|apple", "apple|apple", s"banana|$other", s"cherry|$other"))
  }

  test("outputCols mixes appended and in-place columns with a schema that matches transform") {
    val model = new LumpFeatures()
      .setLumpRules(Map("f1" -> 1, "f2" -> 1))
      .setOutputCols(Map("f2" -> "f2_lumped"))
      .fit(df)
    val declared = model.transformSchema(df.schema)
    assert(declared == model.transform(df).schema)
    assert(declared.fieldNames.toSeq == Seq("f1", "f2", "f2_lumped"))
    assert(dump(model.transform(df), Seq("f2", "f2_lumped")) == List(
      s"blue|$other", s"green|$other", "red|red", "red|red", "red|red"))
    assert(dump(model.transform(df), Seq("f1")) == List(other, other, "apple", "apple", "apple"))
  }

  test("outputCols rejects unknown sources, empty, duplicate, and pre-existing destinations") {
    intercept[IllegalArgumentException] {
      new LumpFeatures().setLumpRules(Map("f1" -> 1)).setOutputCols(Map("nope" -> "x")).fit(df)
    }
    intercept[IllegalArgumentException] {
      new LumpFeatures().setLumpRules(Map("f1" -> 1)).setOutputCols(Map("f1" -> "")).fit(df)
    }
    intercept[IllegalArgumentException] {
      new LumpFeatures().setLumpRules(Map("f1" -> 1, "f2" -> 1))
        .setOutputCols(Map("f1" -> "z", "f2" -> "z")).fit(df)
    }
    intercept[IllegalArgumentException] {
      new LumpFeatures().setLumpRules(Map("f1" -> 1)).setOutputCols(Map("f1" -> "f2")).fit(df)
    }
  }

  test("outputCols survives fit, copy, and a save-load round-trip") {
    val model = new LumpFeatures()
      .setLumpRules(Map("f1" -> 1))
      .setOutputCols(Map("f1" -> "f1_lumped"))
      .fit(df)
    assert(model.getOutputCols == Map("f1" -> "f1_lumped"))
    assert(model.copy(new ParamMap()).getOutputCols == Map("f1" -> "f1_lumped"))
    val path = new File(tmpDir.toFile, "lump-outputcols").toString
    model.write.overwrite().save(path)
    val loaded = LumpFeaturesModel.load(path)
    assert(loaded.getOutputCols == Map("f1" -> "f1_lumped"))
    assertDFEq(loaded.transform(df), model.transform(df))
  }

  test("outputCols accepts the Java HashMap and JSON-string setter overloads") {
    val jmap = new java.util.HashMap[String, String]()
    jmap.put("f1", "f1_lumped")
    assert(new LumpFeatures().setOutputCols(jmap).getOutputCols == Map("f1" -> "f1_lumped"))
    val fromJson = new LumpFeatures().setOutputCols("{\"f1\":\"f1_lumped\"}")
    assert(fromJson.getOutputCols == Map("f1" -> "f1_lumped"))
  }

  test("estimator and model nullability follow handleNull even for a non-nullable input column") {
    val schema = StructType(Seq(StructField("f1", StringType, nullable = false)))
    val rows = Seq(Row("apple"), Row("apple"), Row("banana"))
    val dfNN = spark.createDataFrame(rows.asJava, schema)
    assert(!dfNN.schema("f1").nullable)

    val keepEstimator = new LumpFeatures().setLumpRules(Map("f1" -> 1)).setHandleNull("keep")
    assert(keepEstimator.transformSchema(dfNN.schema)("f1").nullable)
    val keepModel = keepEstimator.fit(dfNN)
    val declaredKeep = keepModel.transformSchema(dfNN.schema)
    assert(declaredKeep == keepModel.transform(dfNN).schema)
    assert(declaredKeep("f1").nullable)
    assert(dump(keepModel.transform(dfNN), Seq("f1")) == List(other, "apple", "apple"))

    val otherEstimator = new LumpFeatures().setLumpRules(Map("f1" -> 1)).setHandleNull("other")
    assert(!otherEstimator.transformSchema(dfNN.schema)("f1").nullable)
    val otherModel = otherEstimator.fit(dfNN)
    val declaredOther = otherModel.transformSchema(dfNN.schema)
    assert(declaredOther == otherModel.transform(dfNN).schema)
    assert(!declaredOther("f1").nullable)
    assert(dump(otherModel.transform(dfNN), Seq("f1")) == List(other, "apple", "apple"))
  }

  test("fit-time otherValue survives save-load and protects observed non-retained categories") {
    val fallback = "fallback"
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 1)).setOtherValue(fallback).fit(df)
    val path = new File(tmpDir.toFile, "lump-fitted-other").toString
    model.write.overwrite().save(path)
    val loaded = LumpFeaturesModel.load(path)
    val state = loaded.getKeptValuesJson("f1").parseJson.asJsObject.fields
    assert(state("otherValue") == JsString(fallback))
    assert(!loaded.hasParam("fittedOtherValue"))
    assert(loaded.getKeptValues("f1") == Seq("apple"))
    intercept[IllegalArgumentException] { loaded.setOtherValue("banana") }
    intercept[IllegalArgumentException] { loaded.set(loaded.otherValue, "banana") }
    assert(loaded.getOtherValue == fallback)
    assertDFEq(loaded.transform(df), model.transform(df))
  }

  test("loading legacy model state initializes and enforces the fitted otherValue") {
    val legacy = new LumpFeaturesModel()
      .setLumpRules(Map("f1" -> 1))
      .setOtherValue("fallback")
      .setKeptValuesJson(Map("f1" -> "{\"topK\":1,\"values\":[\"apple\"]}"))
    assert(!legacy.getKeptValuesJson("f1").parseJson.asJsObject.fields.contains("otherValue"))
    val path = new File(tmpDir.toFile, "lump-legacy-other").toString
    legacy.write.overwrite().save(path)
    val loaded = LumpFeaturesModel.load(path)
    val upgraded = loaded.getKeptValuesJson("f1").parseJson.asJsObject.fields
    assert(upgraded("otherValue") == JsString("fallback"))
    intercept[IllegalArgumentException] { loaded.setOtherValue("banana") }
    intercept[IllegalArgumentException] { loaded.clear(loaded.getParam("keptValuesJson")) }
    assert(loaded.getOtherValue == "fallback")
  }

  test("getKeptValuesAsJson gives language wrappers one document with the learned values") {
    val model = new LumpFeatures().setLumpRules(Map("f1" -> 2, "f2" -> 1)).fit(df)
    val expected = """{"f1":["apple","banana"],"f2":["red"]}"""
    assert(model.getKeptValuesAsJson.parseJson == expected.parseJson)
    val dfAllNull = Seq((null.asInstanceOf[String], "red")).toDF("f1", "f2")
    val empty = new LumpFeatures().setLumpRules(Map("f1" -> 2)).fit(dfAllNull)
    assert(empty.getKeptValuesAsJson.parseJson == """{"f1":[]}""".parseJson)
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
    .setOtherValue(other)
    .setKeptValuesJson(Map(
      "f1" -> LumpFeaturesModel.encodeKept(1, Seq("apple"), other),
      "f2" -> LumpFeaturesModel.encodeKept(1, Seq("red"), other)))
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
