package com.microsoft.azure.synapse.ml.logging

object FeatureNames {
  object CognitiveServices {
    val Anomaly = "cognitive-anomalydetection"
    val BingImage = "cognitive-bingimage"
    val Face = "cognitive-face"
    val Form = "cognitive-form"
    val Language = "cognitive-language"
    val OpenAI = "cognitive-openai"
    val Search = "cognitive-search"
    val Speech = "cognitive-speech"
    val Text = "cognitive-text"
    val Translate = "cognitive-translate"
    val Vision = "cognitive-vision"
  }

  val AutoML = "automl"
  val Causal = "causal"
  val Explainers = "explainers"
  val Featurize = "featurize"
  val Geospatial = "geospatial"
  val Image = "image"
  val IsolationForest = "isolationforest"
  val NearestNeighbor = "nearestneighbor"
  val Recommendation = "recommendation"
  val DeepLearning = "deeplearning"
  val OpenCV = "opencv"
  val LightGBM = "lightgbm"
  val VowpalWabbit = "vowpalwabbit"

  val Core = "core"
}
