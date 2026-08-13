// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.schema

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifySchemaConstants extends TestBase {

  test("score column kind constants have expected values") {
    assert(SchemaConstants.ScoreColumnKind === "ScoreColumnKind")
    assert(SchemaConstants.ScoreValueKind === "ScoreValueKind")
  }

  test("label and score column names have expected values") {
    assert(SchemaConstants.TrueLabelsColumn === "true_labels")
    assert(SchemaConstants.ScoredLabelsColumn === "scored_labels")
    assert(SchemaConstants.ScoresColumn === "scores")
    assert(SchemaConstants.ScoredProbabilitiesColumn === "scored_probabilities")
  }

  test("model and tag constants have expected values") {
    assert(SchemaConstants.ScoreModelPrefix === "score_model")
    assert(SchemaConstants.MMLTag === "mml")
    assert(SchemaConstants.MLlibTag === "ml_attr")
  }

  test("residual column names have expected values") {
    assert(SchemaConstants.TreatmentResidualColumn === "treatment_residual")
    assert(SchemaConstants.OutcomeResidualColumn === "outcome_residual")
  }

  test("categorical metadata tag constants have expected values") {
    assert(SchemaConstants.Ordinal === "ord")
    assert(SchemaConstants.MLlibTypeTag === "type")
    assert(SchemaConstants.ValuesString === "vals")
    assert(SchemaConstants.ValuesInt === "vals_int")
    assert(SchemaConstants.ValuesLong === "vals_long")
    assert(SchemaConstants.ValuesDouble === "vals_double")
    assert(SchemaConstants.ValuesBool === "vals_bool")
    assert(SchemaConstants.HasNullLevels === "null_exists")
  }

  test("ML kind constants have expected values") {
    assert(SchemaConstants.ClassificationKind === "Classification")
    assert(SchemaConstants.RegressionKind === "Regression")
  }

  test("Spark native column name constants have expected values") {
    assert(SchemaConstants.SparkPredictionColumn === "prediction")
    assert(SchemaConstants.SparkRawPredictionColumn === "rawPrediction")
    assert(SchemaConstants.SparkProbabilityColumn === "probability")
  }
}
