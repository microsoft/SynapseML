// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.param

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.{Transformer, Estimator, Model, PipelineStage}
import org.apache.spark.ml.feature.{Tokenizer, HashingTF, StringIndexer, StringIndexerModel}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

class VerifyPipelineStageParams extends TestBase {

  // Test class that holds the params
  private class TestParamsHolder extends Params {
    override val uid: String = "test-holder"

    val transformerParam = new TransformerParam(this, "transformer", "A transformer param")
    val estimatorParam = new EstimatorParam(this, "estimator", "An estimator param")
    val pipelineStageParam = new PipelineStageParam(this, "pipelineStage", "A pipeline stage param")

    override def copy(extra: ParamMap): Params = this
  }

  // TransformerParam tests
  test("TransformerParam can be created with basic constructor") {
    val holder = new TestParamsHolder
    assert(holder.transformerParam.name === "transformer")
    assert(holder.transformerParam.doc === "A transformer param")
  }

  test("TransformerParam accepts valid Transformer") {
    val holder = new TestParamsHolder
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    // Should not throw
    holder.set(holder.transformerParam, tokenizer)
  }

  test("TransformerParam with custom validator") {
    val holder = new Params {
      override val uid: String = "test"
      val validatedParam = new TransformerParam(
        this, "validated", "validated param",
        (t: Transformer) => t.isInstanceOf[Tokenizer]
      )
      override def copy(extra: ParamMap): Params = this
    }
    val tokenizer = new Tokenizer()
    holder.set(holder.validatedParam, tokenizer)
  }

  test("TransformerParam rLoadLine generates correct R code") {
    val holder = new TestParamsHolder
    val rCode = holder.transformerParam.rLoadLine(1)
    assert(rCode.contains("ml_load"))
    assert(rCode.contains("model-1.model"))
    assert(rCode.contains("complexParams"))
    assert(rCode.contains("transformer"))
    assert(rCode.contains("ml_stages"))
  }

  // EstimatorParam tests
  test("EstimatorParam can be created with basic constructor") {
    val holder = new TestParamsHolder
    assert(holder.estimatorParam.name === "estimator")
    assert(holder.estimatorParam.doc === "An estimator param")
  }

  test("EstimatorParam accepts valid Estimator") {
    val holder = new TestParamsHolder
    val indexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel")
    holder.set(holder.estimatorParam, indexer)
  }

  test("EstimatorParam rLoadLine generates correct R code") {
    val holder = new TestParamsHolder
    val rCode = holder.estimatorParam.rLoadLine(2)
    assert(rCode.contains("ml_load"))
    assert(rCode.contains("model-2.model"))
    assert(rCode.contains("complexParams"))
    assert(rCode.contains("estimator"))
  }

  // PipelineStageParam tests
  test("PipelineStageParam can be created with basic constructor") {
    val holder = new TestParamsHolder
    assert(holder.pipelineStageParam.name === "pipelineStage")
    assert(holder.pipelineStageParam.doc === "A pipeline stage param")
  }

  test("PipelineStageParam accepts Transformer") {
    val holder = new TestParamsHolder
    val tokenizer = new Tokenizer()
    holder.set(holder.pipelineStageParam, tokenizer)
  }

  test("PipelineStageParam accepts Estimator") {
    val holder = new TestParamsHolder
    val indexer = new StringIndexer()
    holder.set(holder.pipelineStageParam, indexer)
  }

  test("PipelineStageParam rLoadLine generates correct R code") {
    val holder = new TestParamsHolder
    val rCode = holder.pipelineStageParam.rLoadLine(3)
    assert(rCode.contains("ml_load"))
    assert(rCode.contains("model-3.model"))
    assert(rCode.contains("pipelineStage"))
    assert(rCode.contains("ml_stages"))
  }

  // PipelineStageWrappable trait tests
  test("PipelineStageWrappable pyValue returns model reference") {
    val holder = new TestParamsHolder
    val tokenizer = new Tokenizer()
    val pyVal = holder.transformerParam.pyValue(tokenizer)
    assert(pyVal === "transformerModel")
  }

  test("PipelineStageWrappable pyLoadLine generates Python code") {
    val holder = new TestParamsHolder
    val pyCode = holder.transformerParam.pyLoadLine(1)
    assert(pyCode.contains("Pipeline.load"))
    assert(pyCode.contains("model-1.model"))
    assert(pyCode.contains("complexParams"))
    assert(pyCode.contains("getStages()"))
  }

  test("PipelineStageWrappable rValue returns model reference") {
    val holder = new TestParamsHolder
    val tokenizer = new Tokenizer()
    val rVal = holder.transformerParam.rValue(tokenizer)
    assert(rVal === "transformerModel")
  }

  test("PipelineStageWrappable assertEquality succeeds for same transformer") {
    val holder = new TestParamsHolder
    val t1 = new Tokenizer().setInputCol("a").setOutputCol("b")
    val t2 = new Tokenizer().setInputCol("a").setOutputCol("b")
    // Should not throw
    holder.transformerParam.assertEquality(t1, t2)
  }

  test("PipelineStageWrappable assertEquality throws for non-PipelineStage") {
    val holder = new TestParamsHolder
    assertThrows[AssertionError] {
      holder.transformerParam.assertEquality("not a stage", "also not a stage")
    }
  }
}
