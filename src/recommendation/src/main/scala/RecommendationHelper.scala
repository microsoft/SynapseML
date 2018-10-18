// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.ml.recommendation

import com.microsoft.ml.spark.Wrappable
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol}
import org.apache.spark.ml.util._

trait RecommendationParams extends Wrappable with ALSParams

trait RecEvaluatorParams extends Wrappable
  with HasPredictionCol with HasLabelCol with ComplexParamsWritable
