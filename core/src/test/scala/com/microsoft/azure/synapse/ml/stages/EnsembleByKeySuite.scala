// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.core.test.fuzzing.{TestObject, TransformerFuzzing}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{DenseVector, SQLDataTypes}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Cast, RowOrdering}
import org.apache.spark.sql.functions.{array, col, expr, lit, map, struct}
import org.apache.spark.sql.types.{CalendarIntervalType, DoubleType, MapType, Metadata, StringType,
  StructField, StructType}

class EnsembleByKeySuite extends TestBase with TransformerFuzzing[EnsembleByKey] {

  test("Should work on Dataframes doubles or vectors") {
    val scoreDF = spark.createDataFrame(Seq(
      (0, "foo", 1.0, .1),
      (1, "bar", 4.0, -2.0),
      (1, "bar", 0.0, -3.0)))
      .toDF("label1", "label2", "score1", "score2")

    val va = new VectorAssembler().setInputCols(Array("score1", "score2")).setOutputCol("v1")
    val scoreDF2 = va.transform(scoreDF)

    val t = new EnsembleByKey().setKey("label1").setCol("score1")
    val df1 = t.transform(scoreDF2)
    df1.printSchema()
    assert(df1.collect().map(r => (r.getInt(0), r.getDouble(1))).toSet === Set((1, 2.0), (0, 1.0)))

    val t2 = new EnsembleByKey().setKeys("label1", "label2").setCols("score1", "score2", "v1")
    val df2 = t2.transform(scoreDF2)
    val res2 = df2.select("mean(score1)", "mean(v1)").collect().map(r => (r.getDouble(0), r.getAs[DenseVector](1)))
    val true2 = Set(
      (2.0, new DenseVector(Array(2.0, -2.5))),
      (1.0, new DenseVector(Array(1.0, 0.1))))
    assert(res2.toSet === true2)
  }

  test("should support collapsing or not") {
    val scoreDF = spark.createDataFrame(
        Seq((0, "foo", 1.0, .1),
            (1, "bar", 4.0, -2.0),
            (1, "bar", 0.0, -3.0)))
      .toDF("label1", "label2", "score1", "score2")

    val va = new VectorAssembler().setInputCols(Array("score1", "score2")).setOutputCol("v1")
    val scoreDF2 = va.transform(scoreDF)

    val t = new EnsembleByKey().setKey("label1").setCol("score1").setCollapseGroup(false)
    val df1 = t.transform(scoreDF2)

    assert(df1.collect().map(r => (r.getInt(0), r.getDouble(5))).toSet === Set((1, 2.0), (0, 1.0)))
    assert(df1.count() == scoreDF.count())
    df1.show()
  }

  test("transformSchema should match mixed aggregate output for default and explicit names") {
    val input = mixedTypeDF
    val inputNames = Array("doubleScore", "floatScore", "features")
    val defaultNames = inputNames.map(name => s"mean($name)")
    val explicitNames = Array("averageDouble", "averageFloat", "averageFeatures")
    val keyNames = Array("group", "region")

    assert(input.schema("features").metadata !== Metadata.empty)

    Seq(defaultNames -> false, explicitNames -> true).foreach { case (outputNames, useExplicitNames) =>
      Seq(true, false).foreach { collapseGroup =>
        val transformer = new EnsembleByKey()
          .setKeys(keyNames).setCols(inputNames).setCollapseGroup(collapseGroup)
        if (useExplicitNames) {
          transformer.setColNames(outputNames)
        }

        val transformedSchema = transformer.transformSchema(input.schema)
        val actualSchema = transformer.transform(input).schema
        val expectedNames = if (collapseGroup) {
          keyNames ++ outputNames
        } else {
          keyNames ++ input.columns.filterNot((keyNames ++ outputNames).contains) ++ outputNames
        }

        withClue(s"explicitNames=$useExplicitNames, collapseGroup=$collapseGroup: ") {
          assert(transformedSchema === actualSchema)
          assert(actualSchema.fieldNames === expectedNames)
          assert(actualSchema(outputNames(0)) === StructField(outputNames(0), DoubleType))
          assert(actualSchema(outputNames(1)) === StructField(outputNames(1), DoubleType))
          assert(actualSchema(outputNames(2)) ===
            StructField(outputNames(2), SQLDataTypes.VectorType, nullable = false))
        }
      }
    }
  }

  test("non-collapsed output should overwrite numeric and vector columns") {
    val input = mixedTypeDF
    val overwrittenNames = Array("doubleScore", "floatScore", "features")
    val transformer = new EnsembleByKey()
      .setKeys("group", "region").setCols(overwrittenNames)
      .setColNames(overwrittenNames).setCollapseGroup(false)

    val transformedSchema = transformer.transformSchema(input.schema)
    val transformed = transformer.transform(input)

    assert(transformed.schema === transformedSchema)
    assert(transformed.columns ===
      Array("group", "region", "id", "component1", "component2") ++ overwrittenNames)
    assert(transformed.schema("features").metadata === Metadata.empty)
    assert(!transformed.schema("features").nullable)

    val actual = transformed.orderBy("id")
      .select("doubleScore", "floatScore", "features")
      .collect()
      .map(row => (row.getDouble(0), row.getDouble(1), row.getAs[DenseVector](2)))
    val expected = Array(
      (1.0, 1.0, new DenseVector(Array(1.0, 0.1))),
      (2.0, 2.0, new DenseVector(Array(2.0, -2.5))),
      (2.0, 2.0, new DenseVector(Array(2.0, -2.5))))

    assert(actual === expected)
  }

  test("non-collapsed output should replace case-variant columns consistently") {
    val input = spark.createDataFrame(Seq((0, "group", 1.0, "lower", "upper")))
      .toDF("id", "key", "score", "features", "FEATURES")

    Seq(false -> Array("key", "id", "score", "features"),
        true -> Array("key", "id", "score", "FEATURES", "features"))
      .foreach { case (caseSensitive, expectedNames) =>
        withCaseSensitiveAnalysis(caseSensitive) {
          val transformer = new EnsembleByKey()
            .setKey("key").setCol("score").setColName("features").setCollapseGroup(false)

          val transformedSchema = transformer.transformSchema(input.schema)
          val actualSchema = transformer.transform(input).schema

          assert(transformedSchema === actualSchema)
          assert(actualSchema.fieldNames === expectedNames)
        }
      }
  }

  test("default output names should follow updated input columns before transform") {
    val transformer = new EnsembleByKey().setKeys("group", "region").setCol("doubleScore")
    assert(transformer.getDefault(transformer.colNames).isEmpty)
    transformer.transformSchema(mixedTypeDF.schema)
    assert(transformer.getDefault(transformer.colNames).isEmpty)
    assert(transformer.getColNames === Array("mean(doubleScore)"))
    transformer.transform(mixedTypeDF)
    assert(transformer.getDefault(transformer.colNames).isEmpty)
    transformer.setCols("doubleScore", "floatScore")

    assert(transformer.transformSchema(mixedTypeDF.schema).fieldNames ===
      Array("group", "region", "mean(doubleScore)", "mean(floatScore)"))
    assert(transformer.getColNames === Array("mean(doubleScore)", "mean(floatScore)"))
  }

  test("grouping keys should resolve case-insensitively to input field names") {
    withCaseSensitiveAnalysis(false) {
      Seq(true, false).foreach { collapseGroup =>
        val transformer = new EnsembleByKey()
          .setKeys("GROUP", "REGION").setCol("doubleScore").setCollapseGroup(collapseGroup)

        val transformedSchema = transformer.transformSchema(mixedTypeDF.schema)
        val actualSchema = transformer.transform(mixedTypeDF).schema

        withClue(s"collapseGroup=$collapseGroup: ") {
          assert(transformedSchema === actualSchema)
          assert(actualSchema.fieldNames.take(2) === Array("GROUP", "REGION"))
        }
      }
    }
  }

  test("grouping key resolution should honor case-sensitive analysis") {
    withCaseSensitiveAnalysis(true) {
      val input = spark.createDataFrame(Seq(("lower", "upper", 1.0)))
        .toDF("group", "GROUP", "score")
      val transformer = new EnsembleByKey().setKey("group").setCol("score")

      assert(transformer.transformSchema(input.schema) === transformer.transform(input).schema)

      val error = intercept[IllegalArgumentException] {
        new EnsembleByKey().setKey("Group").setCol("score").transformSchema(input.schema)
      }
      assert(error.getMessage.contains("Group does not exist"))
    }
  }

  test("transformSchema should match output when grouping column retention is disabled") {
    withSQLConf("spark.sql.retainGroupColumns", "false") {
      Seq(true, false).foreach { collapseGroup =>
        val transformer = new EnsembleByKey()
          .setKeys("group", "region").setCol("doubleScore").setCollapseGroup(collapseGroup)
        val transformedSchema = transformer.transformSchema(mixedTypeDF.schema)
        val actualSchema = transformer.transform(mixedTypeDF).schema

        assert(transformedSchema === actualSchema)
        assert(actualSchema.fieldNames.take(2) === Array("group", "region"))
      }
    }
  }

  test("transform should use the dataset session for grouping column retention") {
    val disabledSession = spark.newSession()
    disabledSession.conf.set("spark.sql.retainGroupColumns", false)
    val disabledInput = disabledSession.createDataFrame(Seq(("group", 1.0))).toDF("group", "score")

    withActiveSession(spark) {
      val transformer = new EnsembleByKey().setKey("group").setCol("score")
      assert(transformer.transformSchema(disabledInput.schema) === transformer.transform(disabledInput).schema)
    }

    val enabledSession = spark.newSession()
    enabledSession.conf.set("spark.sql.retainGroupColumns", true)
    val enabledInput = enabledSession.createDataFrame(Seq(("group", 1.0))).toDF("group", "score")

    withSQLConf("spark.sql.retainGroupColumns", "false") {
      withActiveSession(spark) {
        val transformer = new EnsembleByKey().setKey("group").setCol("score")
        val transformed = transformer.transform(enabledInput)
        val pipelineModel = new Pipeline().setStages(Array(transformer)).fit(enabledInput)

        assert(transformer.transformSchema(enabledInput.schema) === transformed.schema)
        assert(pipelineModel.transform(enabledInput).schema === transformed.schema)
        assert(transformed.columns === Array("group", "mean(score)"))
      }
    }
  }

  test("configuration parsing should match Spark boolean parsing") {
    withSQLConf("spark.sql.caseSensitive", " false ") {
      val transformer = new EnsembleByKey().setKey("GROUP").setCol("doubleScore")
      assert(transformer.transformSchema(mixedTypeDF.schema) === transformer.transform(mixedTypeDF).schema)
    }

    withSQLConf("spark.sql.retainGroupColumns", " true ") {
      val transformer = new EnsembleByKey().setKey("group").setCol("doubleScore")
      assert(transformer.transformSchema(mixedTypeDF.schema) === transformer.transform(mixedTypeDF).schema)
    }

    withSQLConf("spark.sql.retainGroupColumns", " false ") {
      val transformer = new EnsembleByKey().setKey("group").setCol("doubleScore")
      assert(transformer.transformSchema(mixedTypeDF.schema) === transformer.transform(mixedTypeDF).schema)
    }
  }

  test("no active session should expose the documented case-resolution limitation") {
    withSQLConf("spark.sql.caseSensitive", "true") {
      val input = spark.createDataFrame(Seq((0, "group", 1.0, 2.0, 3.0)))
        .toDF("id", "key", "score", "features", "FEATURES")
      val transformer = new EnsembleByKey()
        .setKey("key").setCol("score").setColName("features").setCollapseGroup(false)
      val assembler = new VectorAssembler()
        .setInputCols(Array("FEATURES")).setOutputCol("vector")
      val pipeline = new Pipeline().setStages(Array(transformer, assembler))

      withoutActiveSession {
        val transformedSchema = transformer.transformSchema(input.schema)
        val actualSchema = transformer.transform(input).schema

        assert(transformedSchema.fieldNames === Array("key", "id", "score", "features"))
        assert(actualSchema.fieldNames === Array("key", "id", "score", "FEATURES", "features"))
        val pipelineError = intercept[IllegalArgumentException](pipeline.fit(input))
        assert(pipelineError.getMessage.contains("FEATURES does not exist"))
      }
      pipeline.fit(input)
    }
  }

  test("transform should use the dataset session for column resolution") {
    val sensitiveSession = spark.newSession()
    sensitiveSession.conf.set("spark.sql.caseSensitive", true)
    val sensitiveInput = sensitiveSession.createDataFrame(Seq(("group", 1.0))).toDF("group", "score")

    withCaseSensitiveAnalysis(false) {
      val transformer = new EnsembleByKey().setKey("GROUP").setCol("SCORE")
      assert(transformer.transformSchema(sensitiveInput.schema).fieldNames === Array("GROUP", "mean(SCORE)"))
      assert(intercept[IllegalArgumentException](transformer.transform(sensitiveInput))
        .getMessage.contains("does not exist"))
    }

    val insensitiveSession = spark.newSession()
    insensitiveSession.conf.set("spark.sql.caseSensitive", false)
    val insensitiveInput = insensitiveSession.createDataFrame(Seq(("group", 1.0))).toDF("group", "score")

    withCaseSensitiveAnalysis(true) {
      val transformer = new EnsembleByKey().setKey("GROUP").setCol("SCORE")
      assert(transformer.transform(insensitiveInput).schema.fieldNames === Array("GROUP", "mean(SCORE)"))
    }

    withoutActiveSession {
      val transformer = new EnsembleByKey().setKey("GROUP").setCol("SCORE")
      assert(intercept[IllegalArgumentException](transformer.transform(sensitiveInput))
        .getMessage.contains("does not exist"))
    }
  }

  test("nested and quoted field references should match Spark resolution") {
    val nestedInput = spark.createDataFrame(Seq(("a", 1.0), ("a", 3.0)))
      .toDF("nestedKey", "score")
      .select(struct(col("nestedKey").alias("key")).alias("nested"), col("score"))

    Seq(true, false).foreach { collapseGroup =>
      val transformer = new EnsembleByKey()
        .setKey("nested.key").setCol("score").setCollapseGroup(collapseGroup)
      val transformedSchema = transformer.transformSchema(nestedInput.schema)
      val transformed = transformer.transform(nestedInput)

      assert(transformedSchema === transformed.schema)
      assert(transformed.schema.fieldNames.head === "key")
      assert(transformed.select("mean(score)").head().getDouble(0) === 2.0)
    }

    val dottedInput = spark.createDataFrame(Seq(("a", 1.0), ("a", 3.0))).toDF("a.b", "score")
    val dottedTransformer = new EnsembleByKey().setKey("`a.b`").setCol("score")

    assert(dottedTransformer.transformSchema(dottedInput.schema) === dottedTransformer.transform(dottedInput).schema)
  }

  test("nested key nullability should include nullable ancestor structs") {
    val inputSchema = StructType(Array(
      StructField(
        "nested",
        StructType(Array(StructField("key", StringType, nullable = false))),
        nullable = true),
      StructField("score", DoubleType, nullable = false)))
    val rows = java.util.Arrays.asList(
      Row(Row("a"), 1.0),
      Row(Row("a"), 3.0))
    val input = spark.createDataFrame(rows, inputSchema)

    Seq(true, false).foreach { collapseGroup =>
      val transformer = new EnsembleByKey()
        .setKey("nested.key").setCol("score").setCollapseGroup(collapseGroup)
      val transformedSchema = transformer.transformSchema(input.schema)
      val actualSchema = transformer.transform(input).schema

      assert(transformedSchema === actualSchema)
      assert(actualSchema("key").nullable)
    }
  }

  test("non-collapsed nested keys should reject unsafe leaf-name collisions") {
    val collisionInput = spark.createDataFrame(Seq(("row-1", "group", 1.0)))
      .toDF("id", "nestedId", "score")
      .select(col("id"), struct(col("nestedId").alias("id")).alias("meta"), col("score"))
    val collisionTransformer = new EnsembleByKey()
      .setKey("meta.id").setCol("score").setCollapseGroup(false)

    assertConsistentSchemaError(
      collisionTransformer, collisionInput, "ambiguous between a nested field and a dataset qualifier")

    val duplicateInput = spark.createDataFrame(Seq(("left", "right", 1.0)))
      .toDF("leftKey", "rightKey", "score")
      .select(
        struct(col("leftKey").alias("key")).alias("left"),
        struct(col("rightKey").alias("key")).alias("right"),
        col("score"))
    val duplicateTransformer = new EnsembleByKey()
      .setKeys("left.key", "right.key").setCol("score").setCollapseGroup(false)

    assertConsistentSchemaError(duplicateTransformer, duplicateInput, "must resolve to distinct output columns")
  }

  test("non-collapsed duplicate grouping keys should fail consistently") {
    Seq("true", "false").foreach { retainGroupColumns =>
      withSQLConf("spark.sql.retainGroupColumns", retainGroupColumns) {
        val transformer = new EnsembleByKey()
          .setKeys("group", "group").setCol("doubleScore").setCollapseGroup(false)

        assertConsistentSchemaError(
          transformer,
          mixedTypeDF,
          "must resolve to distinct output columns")
      }
    }

    val collapsed = new EnsembleByKey()
      .setKeys("group", "group").setCol("doubleScore").setCollapseGroup(true)
    assert(collapsed.transformSchema(mixedTypeDF.schema) === collapsed.transform(mixedTypeDF).schema)
  }

  test("nested keys should preserve unreferenced duplicate top-level columns") {
    val input = spark.createDataFrame(Seq(("group", 1.0, 10.0)))
      .toDF("key", "score", "duplicate")
      .select(
        struct(col("key").alias("value")).alias("nested"),
        col("score"),
        col("duplicate").alias("duplicate"),
        col("duplicate").alias("duplicate"))
    val transformer = new EnsembleByKey()
      .setKey("nested.value").setCol("score").setCollapseGroup(false)

    val transformed = transformer.transform(input)
    assert(transformer.transformSchema(input.schema) === transformed.schema)
    assert(transformed.schema.fieldNames ===
      Array("value", "nested", "score", "duplicate", "duplicate", "mean(score)"))
  }

  test("quoted field references should ignore quoted-regex column settings") {
    withSQLConf("spark.sql.parser.quotedRegexColumnNames", "true") {
      val keyInput = spark.createDataFrame(Seq(("a", 1.0), ("a", 3.0))).toDF("a.b", "score")
      val keyTransformer = new EnsembleByKey().setKey("`a.b`").setCol("score")
      assert(keyTransformer.transformSchema(keyInput.schema) === keyTransformer.transform(keyInput).schema)

      val colInput = spark.createDataFrame(Seq(("group", 1.0), ("group", 3.0))).toDF("group", "s.c")
      val colTransformer = new EnsembleByKey().setKey("group").setCol("`s.c`")
      assert(colTransformer.transformSchema(colInput.schema) === colTransformer.transform(colInput).schema)
    }
  }

  test("literal dotted aggregate columns should require Spark quoting") {
    val input = spark.createDataFrame(Seq(("group", 1.0), ("group", 3.0))).toDF("group", "s.c")
    val quotedTransformer = new EnsembleByKey().setKey("group").setCol("`s.c`")

    assert(quotedTransformer.transformSchema(input.schema) === quotedTransformer.transform(input).schema)

    val plainTransformer = new EnsembleByKey().setKey("group").setCol("s.c")
    assertConsistentSchemaError(plainTransformer, input, "s.c does not exist")
  }

  test("qualified and collection field references should match Spark resolution") {
    val qualifiedInput = mixedTypeDF.as("source")
    val qualifiedTransformer = new EnsembleByKey().setKey("source.group").setCol("doubleScore")
    assert(qualifiedTransformer.transformSchema(qualifiedInput.schema) ===
      qualifiedTransformer.transform(qualifiedInput).schema)

    val collectionBase = spark.createDataFrame(Seq(("group", 1.0))).toDF("key", "score")
    val arrayInput = collectionBase.select(
      array(struct(col("key").alias("field"))).alias("items"),
      col("score"))
    val arrayTransformer = new EnsembleByKey().setKey("items.field").setCol("score")
    val arrayResult = arrayTransformer.transform(arrayInput)
    assert(arrayTransformer.transformSchema(arrayInput.schema) === arrayResult.schema)
    assert(arrayResult.collect().head.getSeq[String](0) === Seq("group"))

    val nullableArrayInput = collectionBase.select(
      array(struct(expr("CAST(NULL AS STRING)").alias("field"))).alias("items"),
      col("score"))
    val nullableArrayTransformer = new EnsembleByKey().setKey("items.field").setCol("score")
    val nullableArrayResult = nullableArrayTransformer.transform(nullableArrayInput)
    assert(nullableArrayTransformer.transformSchema(nullableArrayInput.schema) === nullableArrayResult.schema)
    assert(Option(nullableArrayResult.collect().head.getSeq[String](0).head).isEmpty)

    val mapInput = collectionBase.select(
      map(lit("field"), col("key")).alias("values"),
      col("score"))
    val mapTransformer = new EnsembleByKey().setKey("values.field").setCol("score")
    assert(mapTransformer.transformSchema(mapInput.schema) === mapTransformer.transform(mapInput).schema)

    val invalidMapInput = collectionBase.select(
      map(struct(lit(1).alias("part")), col("key")).alias("values"),
      col("score"))
    val invalidMapTransformer = new EnsembleByKey().setKey("values.field").setCol("score")
    assertConsistentSchemaError(invalidMapTransformer, invalidMapInput, "does not accept string keys")
  }

  test("map key extraction should follow Spark cast coercion") {
    val base = spark.createDataFrame(Seq(("group", 1.0), ("group", 3.0))).toDF("key", "score")
    Seq(
      "values.true" -> map(lit(true), col("key")),
      "values.field" -> map(lit("field").cast("binary"), col("key")),
      "values.1" -> map(lit(1), col("key")),
      "values.2020-01-01" -> map(lit("2020-01-01").cast("date"), col("key"))
    ).foreach { case (reference, values) =>
      val input = base.select(values.alias("values"), col("score"))
      val transformed = assertSchemaAgrees(new EnsembleByKey().setKey(reference).setCol("score"), input)
      withClue(s"$reference: ") {
        assert(transformed.head().getString(0) === "group")
        assert(transformed.select("mean(score)").head().getDouble(0) === 2.0)
      }
    }
  }

  test("map keys Spark cannot order should be rejected consistently") {
    val base = spark.createDataFrame(Seq(("group", 1.0), ("group", 3.0))).toDF("key", "score")
    val input = base.select(
      map(expr("make_interval(0, 0, 0, 1, 0, 0, 0)"), col("key")).alias("values"),
      col("score"))
    val keyType = input.schema("values").dataType.asInstanceOf[MapType].keyType

    assert(keyType === CalendarIntervalType)
    assert(Cast.canCast(StringType, keyType), "the key type is castable from a string literal")
    assert(!RowOrdering.isOrderable(keyType), "the key type is not orderable, so GetMapValue fails")
    intercept[AnalysisException](input.select(expr("values[make_interval(0, 0, 0, 1, 0, 0, 0)]")).schema)

    val transformer = new EnsembleByKey().setKey("values.1 days").setCol("score")
    assertConsistentSchemaError(transformer, input, "map key type CalendarIntervalType is not orderable")
    assertConsistentSchemaError(transformer, input, "Use a map column whose key type is orderable")
  }

  test("extracted grouping values Spark cannot order should be rejected consistently") {
    val input = spark.range(1).select(
      map(lit("outer"), map(lit("inner"), lit(1))).alias("values"),
      lit(1.0).alias("score"))
    val transformer = new EnsembleByKey().setKey("values.outer").setCol("score")

    assertConsistentSchemaError(transformer, input, "Spark cannot use as a grouping key")
  }

  test("map extraction should reject dataset qualifier collisions") {
    val input = spark.createDataFrame(Seq(("group", 1.0)))
      .toDF("key", "score")
      .select(map(lit("field"), col("score")).alias("values"), col("score"), col("key").alias("field"))
      .as("values")
    val transformer = new EnsembleByKey().setKey("values.field").setCol("score")

    assertConsistentSchemaError(transformer, input, "ambiguous between a nested field and a dataset qualifier")
  }

  test("nested key output names should preserve configured casing") {
    val input = spark.createDataFrame(Seq(("group", 1.0)))
      .toDF("key", "score")
      .select(struct(col("key").alias("Key")).alias("nested"), col("score"))
    val transformer = new EnsembleByKey().setKey("nested.key").setCol("score")

    assert(transformer.transformSchema(input.schema) === transformer.transform(input).schema)
    assert(transformer.transform(input).schema.fieldNames.head === "key")
  }

  test("qualified references should preserve qualifier identity") {
    val left = spark.createDataFrame(Seq((1, "left", 1.0))).toDF("id", "group", "score").as("left")
    val right = spark.createDataFrame(Seq((1, "right"))).toDF("id", "group").as("right")
    val joined = left.join(right, Seq("id"))

    Seq("left", "right").foreach { qualifier =>
      val transformer = new EnsembleByKey().setKey(s"$qualifier.group").setCol("score")
      withClue(s"$qualifier: ") {
        assert(assertSchemaAgrees(transformer, joined).head().getString(0) === qualifier)
      }
    }

    assertConsistentSchemaError(
      new EnsembleByKey().setKey("right.group").setCol("score").setCollapseGroup(false),
      joined,
      "multiple columns are named group when collapseGroup is false")

    val invalidQualifier = new EnsembleByKey().setKey("wrong.group").setCol("score")
    assert(invalidQualifier.transformSchema(joined.schema).fieldNames === Array("group", "mean(score)"))
    val error = intercept[IllegalArgumentException](invalidQualifier.transform(joined))
    assert(error.getMessage.contains("does not match a dataset qualifier"))
  }

  test("non-collapsed qualified references should preserve unrelated duplicates") {
    val left = spark.createDataFrame(Seq((1, "group", 1.0))).toDF("id", "group", "score").as("left")
    val right = spark.createDataFrame(Seq((1, 2.0))).toDF("id", "score").as("right")
    val joined = left.join(right, Seq("id"))
    val transformer = new EnsembleByKey()
      .setKey("left.group").setCol("left.score")
      .setColName("average").setCollapseGroup(false)
    val transformed = assertSchemaAgrees(transformer, joined)

    assert(transformed.schema.fieldNames === Array("group", "id", "score", "score", "average"))
    assert(transformed.head().getDouble(4) === 1.0)
  }

  test("qualified aggregates should compare derived aggregate outputs") {
    val left = spark.createDataFrame(Seq((1, "group", 1.0), (2, "group", 3.0)))
      .toDF("id", "group", "score").as("left")
    val right = spark.createDataFrame(Seq((1, 5.0))).toDF("id", "score").as("right")
    val joined = left.join(right, Seq("id"), "left_outer")
    assert(joined.schema.fields.filter(_.name == "score").map(_.nullable) === Array(false, true))

    Seq("left.score" -> 2.0, "right.score" -> 5.0).foreach { case (reference, expected) =>
      val transformed = assertSchemaAgrees(new EnsembleByKey().setKey("group").setCol(reference), joined)
      withClue(s"$reference: ") {
        assert(transformed.schema.last === StructField(s"mean($reference)", DoubleType))
        assert(transformed.head().getDouble(1) === expected)
      }
    }

    assertConsistentSchemaError(
      new EnsembleByKey().setKey("right.score").setCol("left.score"),
      joined,
      "incompatible declared outputs")

    val nestedLeft = spark.createDataFrame(Seq((1, 1.0), (1, 3.0))).toDF("id", "value")
      .select(col("id"), struct(col("value")).alias("s")).as("left")
    val nestedRight = spark.createDataFrame(Seq((1, 5.0f))).toDF("id", "value")
      .select(col("id"), struct(col("value")).alias("s")).as("right")
    val nested = assertSchemaAgrees(
      new EnsembleByKey().setKey("id").setCol("right.s.value"),
      nestedLeft.join(nestedRight, Seq("id")))
    assert(nested.schema.last === StructField("mean(right.s.value)", DoubleType))
    assert(nested.head().getDouble(1) === 5.0)

    val stringRight = spark.createDataFrame(Seq((1, "5"))).toDF("id", "value")
      .select(col("id"), struct(col("value")).alias("s")).as("right")
    assertConsistentSchemaError(
      new EnsembleByKey().setKey("id").setCol("right.s.value"),
      nestedLeft.join(stringRight, Seq("id")),
      "incompatible declared outputs")
  }

  test("multipart qualifiers should agree with schema-only interpretations") {
    val base = spark.createDataFrame(Seq(("top", "nested", 1.0), ("top", "nested", 3.0)))
      .toDF("group", "nestedGroup", "score")
    val viewName = s"ensembleView${System.nanoTime()}"
    val input = base.select(
      col("group"), struct(col("nestedGroup").alias("group")).alias(viewName), col("score"))
    val transformer = new EnsembleByKey().setKey(s"global_temp.$viewName.group").setCol("score")

    assert(assertSchemaAgrees(transformer, input.as("global_temp")).head().getString(0) === "nested")

    input.createOrReplaceGlobalTempView(viewName)
    try {
      val view = spark.table(s"global_temp.$viewName")
      assert(assertSchemaAgrees(transformer, view).head().getString(0) === "top")
    } finally {
      spark.catalog.dropGlobalTempView(viewName)
    }

    val conflicting = base.select(
      col("score").alias("group"),
      struct(col("nestedGroup").alias("group")).alias("view"),
      col("score"))
    assertConsistentSchemaError(
      new EnsembleByKey().setKey("global_temp.view.group").setCol("score"),
      conflicting,
      "ambiguous between a nested field and a dataset qualifier")
  }

  test("schema and runtime should reject invalid column configurations") {
    val invalidConfigurations = Seq(
      new EnsembleByKey().setCol("doubleScore") -> "keys must be set and non-empty",
      new EnsembleByKey().setKeys(Array.empty[String]).setCol("doubleScore") ->
        "keys must be set and non-empty",
      new EnsembleByKey().setKey("group") -> "cols must be set and non-empty",
      new EnsembleByKey().setKey("group").setCols(Array.empty[String]) ->
        "cols must be set and non-empty",
      new EnsembleByKey().setKey("missingKey").setCol("doubleScore") -> "missingKey does not exist",
      new EnsembleByKey().setKey("group").setCol("missingCol") -> "missingCol does not exist",
      new EnsembleByKey().setKey("group").setCols("doubleScore", "floatScore")
        .setColName("average") -> "must have the same length",
      new EnsembleByKey().setKey("group").setCol("doubleScore").setColName("GROUP")
        .setCollapseGroup(false) -> "cannot overwrite grouping keys"
    )

    invalidConfigurations.foreach { case (transformer, expectedMessage) =>
      assertConsistentSchemaError(transformer, mixedTypeDF, expectedMessage)
    }

    withCaseSensitiveAnalysis(false) {
      val ambiguousInput = spark.createDataFrame(Seq(("lower", "upper", 1.0)))
        .toDF("group", "GROUP", "score")
      val keyTransformer = new EnsembleByKey().setKey("group").setCol("score")
      assert(keyTransformer.transformSchema(ambiguousInput.schema).fieldNames ===
        Array("group", "mean(score)"))
      val error = intercept[IllegalArgumentException](keyTransformer.transform(ambiguousInput))
      assert(error.getMessage.contains("group is ambiguous"))

      val ambiguousAggregateInput = spark.createDataFrame(Seq(("group", 1.0, 2.0)))
        .toDF("group", "score", "SCORE")
      val aggregateTransformer = new EnsembleByKey().setKey("group").setCol("score")
      assert(aggregateTransformer.transformSchema(ambiguousAggregateInput.schema).fieldNames ===
        Array("group", "mean(score)"))
      val aggregateError =
        intercept[IllegalArgumentException](aggregateTransformer.transform(ambiguousAggregateInput))
      assert(aggregateError.getMessage.contains("score is ambiguous"))
    }
  }

  test("transformSchema should reject unsupported aggregate types") {
    val input = spark.createDataFrame(Seq(("foo", 1))).toDF("group", "score")
    val transformer = new EnsembleByKey().setKey("group").setCol("score")

    val error = intercept[IllegalArgumentException] {
      transformer.transformSchema(input.schema)
    }

    assert(error.getMessage === "Cannot operate on type IntegerType with strategy mean")
  }

  lazy val testDF: DataFrame = {
    val initialTestDF = spark.createDataFrame(
      Seq((0, "foo", 1.0, .1),
        (1, "bar", 4.0, -2.0),
        (1, "bar", 0.0, -3.0)))
      .toDF("label1", "label2", "score1", "score2")

    new VectorAssembler().setInputCols(Array("score1", "score2"))
      .setOutputCol("v1").transform(initialTestDF)
  }

  lazy val mixedTypeDF: DataFrame = {
    val initialTestDF = spark.createDataFrame(
      Seq((0, "west", "foo", 1.0, 1.0f, 1.0, 0.1),
          (1, "east", "bar", 4.0, 4.0f, 4.0, -2.0),
          (2, "east", "bar", 0.0, 0.0f, 0.0, -3.0)))
      .toDF("id", "region", "group", "doubleScore", "floatScore", "component1", "component2")

    new VectorAssembler()
      .setInputCols(Array("component1", "component2"))
      .setOutputCol("features")
      .transform(initialTestDF)
  }

  lazy val testModel: EnsembleByKey = new EnsembleByKey().setKey("label1").setCol("score1")
      .setCollapseGroup(false).setVectorDims(Map("v1"->2))

  test("should support passing the vector dims to avoid maerialization") {
    val df1 = testModel.transform(testDF)
    assert(df1.collect().map(r => (r.getInt(0), r.getDouble(5))).toSet === Set((1, 2.0), (0, 1.0)))
    assert(df1.count() == testDF.count())
    df1.show()
  }

  test("should overwrite a column if instructed") {
    val scoreDF = spark.createDataFrame(
        Seq((0, "foo", 1.0, .1),
            (1, "bar", 4.0, -2.0),
            (1, "bar", 0.0, -3.0)))
      .toDF("label1", "label2", "score1", "score2")

    val va = new VectorAssembler().setInputCols(Array("score1", "score2")).setOutputCol("v1")
    val scoreDF2 = va.transform(scoreDF)

    val t = new EnsembleByKey().setKey("label1").setCol("score1").setColName("score1").setCollapseGroup(false)
    val df1 = t.transform(scoreDF2)

    assert(scoreDF2.columns.toSet === df1.columns.toSet)

  }

  test("should roundtrip serialize") {
    testSerialization()
  }

  def testObjects(): Seq[TestObject[EnsembleByKey]] = Seq(new TestObject(testModel, testDF))

  def reader: EnsembleByKey.type = EnsembleByKey

  private def withCaseSensitiveAnalysis[T](value: Boolean)(action: => T): T = {
    withSQLConf("spark.sql.caseSensitive", value.toString)(action)
  }

  private def withSQLConf[T](configName: String, value: String)(action: => T): T = {
    val previousValue = spark.conf.get(configName)
    spark.conf.set(configName, value)
    try action finally spark.conf.set(configName, previousValue)
  }

  private def withActiveSession[T](session: SparkSession)(action: => T): T = {
    val previousSession = SparkSession.getActiveSession
    SparkSession.setActiveSession(session)
    try action finally {
      previousSession.fold(SparkSession.clearActiveSession())(SparkSession.setActiveSession)
    }
  }

  private def withoutActiveSession[T](action: => T): T = {
    val previousSession = SparkSession.getActiveSession
    SparkSession.clearActiveSession()
    try action finally previousSession.foreach(SparkSession.setActiveSession)
  }

  private def assertSchemaAgrees(transformer: EnsembleByKey, input: DataFrame): DataFrame = {
    val transformed = transformer.transform(input)
    assert(transformer.transformSchema(input.schema) === transformed.schema)
    transformed
  }

  private def assertConsistentSchemaError(
      transformer: EnsembleByKey,
      input: DataFrame,
      expectedMessage: String
  ): Unit = {
    val schemaError = intercept[IllegalArgumentException](transformer.transformSchema(input.schema))
    val transformError = intercept[IllegalArgumentException](transformer.transform(input))
    assert(schemaError.getMessage.contains(expectedMessage))
    assert(transformError.getMessage.contains(expectedMessage))
  }
}
