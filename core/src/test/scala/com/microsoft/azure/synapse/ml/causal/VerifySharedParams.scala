// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.causal

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.Identifiable

class VerifySharedParams extends TestBase {

  // Test implementation that mixes in all the traits
  private class TestParamsImpl(override val uid: String)
    extends Params
      with HasTreatmentCol
      with HasOutcomeCol
      with HasPostTreatmentCol
      with HasUnitCol
      with HasTimeCol {
    override def copy(extra: org.apache.spark.ml.param.ParamMap): Params = this
  }

  test("HasTreatmentCol sets and gets treatment column") {
    val params = new TestParamsImpl(Identifiable.randomUID("test"))
    params.setTreatmentCol("treatment")
    assert(params.getTreatmentCol === "treatment")
  }

  test("HasTreatmentCol param has correct name and doc") {
    val params = new TestParamsImpl(Identifiable.randomUID("test"))
    assert(params.treatmentCol.name === "treatmentCol")
    assert(params.treatmentCol.doc === "treatment column")
  }

  test("HasOutcomeCol sets and gets outcome column") {
    val params = new TestParamsImpl(Identifiable.randomUID("test"))
    params.setOutcomeCol("outcome")
    assert(params.getOutcomeCol === "outcome")
  }

  test("HasOutcomeCol param has correct name and doc") {
    val params = new TestParamsImpl(Identifiable.randomUID("test"))
    assert(params.outcomeCol.name === "outcomeCol")
    assert(params.outcomeCol.doc === "outcome column")
  }

  test("HasPostTreatmentCol sets and gets post treatment column") {
    val params = new TestParamsImpl(Identifiable.randomUID("test"))
    params.setPostTreatmentCol("postTreatment")
    assert(params.getPostTreatmentCol === "postTreatment")
  }

  test("HasPostTreatmentCol param has correct name and doc") {
    val params = new TestParamsImpl(Identifiable.randomUID("test"))
    assert(params.postTreatmentCol.name === "postTreatmentCol")
    assert(params.postTreatmentCol.doc === "post treatment indicator column")
  }

  test("HasUnitCol sets and gets unit column") {
    val params = new TestParamsImpl(Identifiable.randomUID("test"))
    params.setUnitCol("userId")
    assert(params.getUnitCol === "userId")
  }

  test("HasUnitCol param has correct name") {
    val params = new TestParamsImpl(Identifiable.randomUID("test"))
    assert(params.unitCol.name === "unitCol")
    assert(params.unitCol.doc.contains("identifier for each observed unit"))
  }

  test("HasTimeCol sets and gets time column") {
    val params = new TestParamsImpl(Identifiable.randomUID("test"))
    params.setTimeCol("date")
    assert(params.getTimeCol === "date")
  }

  test("HasTimeCol param has correct name") {
    val params = new TestParamsImpl(Identifiable.randomUID("test"))
    assert(params.timeCol.name === "timeCol")
    assert(params.timeCol.doc.contains("time when outcome is measured"))
  }

  test("All params can be set together") {
    val params = new TestParamsImpl(Identifiable.randomUID("test"))
    params
      .setTreatmentCol("treatment")
      .setOutcomeCol("outcome")
      .setPostTreatmentCol("post")
      .setUnitCol("user")
      .setTimeCol("time")

    assert(params.getTreatmentCol === "treatment")
    assert(params.getOutcomeCol === "outcome")
    assert(params.getPostTreatmentCol === "post")
    assert(params.getUnitCol === "user")
    assert(params.getTimeCol === "time")
  }

  test("Setters return this.type for chaining") {
    val params = new TestParamsImpl(Identifiable.randomUID("test"))
    val result = params.setTreatmentCol("treatment")
    assert(result eq params)
  }
}
