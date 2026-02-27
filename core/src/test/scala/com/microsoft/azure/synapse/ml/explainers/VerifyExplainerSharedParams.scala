// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

class VerifyExplainerSharedParams extends TestBase {

  // Test implementation that mixes in all the traits
  private class TestExplainerParams(override val uid: String)
    extends Params
      with HasMetricsCol
      with HasNumSamples
      with HasTokensCol
      with HasSuperpixelCol
      with HasSamplingFraction
      with HasExplainTarget {
    override def copy(extra: ParamMap): Params = this
  }

  test("CanValidateSchema trait has default implementation") {
    val validator = new CanValidateSchema {}
    // Should not throw
    validator.validateSchema(StructType(Seq()))
  }

  test("HasMetricsCol sets and gets metrics column") {
    val params = new TestExplainerParams(Identifiable.randomUID("test"))
    params.setMetricsCol("metrics_output")
    assert(params.getMetricsCol === "metrics_output")
  }

  test("HasMetricsCol param has correct name and doc") {
    val params = new TestExplainerParams(Identifiable.randomUID("test"))
    assert(params.metricsCol.name === "metricsCol")
    assert(params.metricsCol.doc.contains("fitting metrics"))
  }

  test("HasMetricsCol validates schema rejects duplicate column") {
    val params = new TestExplainerParams(Identifiable.randomUID("test"))
    params.setMetricsCol("existing_col")
    val schema = StructType(Seq(
      StructField("existing_col", StringType)
    ))
    assertThrows[IllegalArgumentException] {
      params.validateSchema(schema)
    }
  }

  test("HasNumSamples sets and gets number of samples") {
    val params = new TestExplainerParams(Identifiable.randomUID("test"))
    params.setNumSamples(100)
    assert(params.getNumSamples === 100)
  }

  test("HasNumSamples getNumSamplesOpt returns Some when set") {
    val params = new TestExplainerParams(Identifiable.randomUID("test"))
    params.setNumSamples(50)
    assert(params.getNumSamplesOpt === Some(50))
  }

  test("HasNumSamples getNumSamplesOpt returns None when not set") {
    val params = new TestExplainerParams(Identifiable.randomUID("test"))
    assert(params.getNumSamplesOpt.isEmpty)
  }

  test("HasNumSamples validates numSamples must be positive") {
    val params = new TestExplainerParams(Identifiable.randomUID("test"))
    assertThrows[IllegalArgumentException] {
      params.setNumSamples(0)
    }
    assertThrows[IllegalArgumentException] {
      params.setNumSamples(-1)
    }
  }

  test("HasTokensCol sets and gets tokens column") {
    val params = new TestExplainerParams(Identifiable.randomUID("test"))
    params.setTokensCol("tokens")
    assert(params.getTokensCol === "tokens")
  }

  test("HasTokensCol param has correct name and doc") {
    val params = new TestExplainerParams(Identifiable.randomUID("test"))
    assert(params.tokensCol.name === "tokensCol")
    assert(params.tokensCol.doc.contains("tokens"))
  }

  test("HasSuperpixelCol sets and gets superpixel column") {
    val params = new TestExplainerParams(Identifiable.randomUID("test"))
    params.setSuperpixelCol("superpixels")
    assert(params.getSuperpixelCol === "superpixels")
  }

  test("HasSuperpixelCol param has correct name and doc") {
    val params = new TestExplainerParams(Identifiable.randomUID("test"))
    assert(params.superpixelCol.name === "superpixelCol")
    assert(params.superpixelCol.doc.contains("superpixel"))
  }

  test("HasSamplingFraction sets and gets sampling fraction") {
    val params = new TestExplainerParams(Identifiable.randomUID("test"))
    params.setSamplingFraction(0.5)
    assert(params.getSamplingFraction === 0.5)
  }

  test("HasSamplingFraction validates fraction in range 0 to 1") {
    val params = new TestExplainerParams(Identifiable.randomUID("test"))
    // Valid values
    params.setSamplingFraction(0.0)
    params.setSamplingFraction(1.0)
    params.setSamplingFraction(0.5)

    // Invalid values
    assertThrows[IllegalArgumentException] {
      params.setSamplingFraction(-0.1)
    }
    assertThrows[IllegalArgumentException] {
      params.setSamplingFraction(1.1)
    }
  }

  test("HasExplainTarget has default targetCol value") {
    val params = new TestExplainerParams(Identifiable.randomUID("test"))
    assert(params.getTargetCol === "probability")
  }

  test("HasExplainTarget sets and gets targetCol") {
    val params = new TestExplainerParams(Identifiable.randomUID("test"))
    params.setTargetCol("prediction")
    assert(params.getTargetCol === "prediction")
  }

  test("HasExplainTarget has default empty targetClasses") {
    val params = new TestExplainerParams(Identifiable.randomUID("test"))
    assert(params.getTargetClasses.isEmpty)
  }

  test("HasExplainTarget sets and gets targetClasses") {
    val params = new TestExplainerParams(Identifiable.randomUID("test"))
    params.setTargetClasses(Array(0, 1, 2))
    assert(params.getTargetClasses === Array(0, 1, 2))
  }

  test("HasExplainTarget sets and gets targetClassesCol") {
    val params = new TestExplainerParams(Identifiable.randomUID("test"))
    params.setTargetClassesCol("classes")
    assert(params.getTargetClassesCol === "classes")
  }
}
