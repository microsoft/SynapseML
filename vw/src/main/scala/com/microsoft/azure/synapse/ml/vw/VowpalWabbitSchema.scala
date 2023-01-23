// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

object VowpalWabbitSchema {
  object Predictions {
    case class Scalars(predictions: Array[Float])

    case class Multiclass(prediction: Int)

    case class Prob(prediction: Float)

    case class Multilabels(prediction: Array[Int])

    case class Scalar(prediction: Float, confidence: Float)

    case class ActionScores(predictions: Array[ActionScore])

    case class ActionScore(action: Int, score: Float)

    case class ActionProbs(predictions: Array[ActionProb])

    case class ActionProb(action: Int, probability: Float)

    case class DecisionProbs(predictions: Array[Array[ActionScore]])

    case class ActionPDFValue(action: Float, pdf: Float)

    case class PDF(segments: Array[PDFSegment])

    case class PDFSegment(left: Float, right: Float, pdfValue: Float)
  }
}
