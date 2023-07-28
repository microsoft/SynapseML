package com.microsoft.azure.synapse.ml.logging.Usage

import scala.collection.mutable.Map

case class FeatureUsagePayload(feature_name: UsageFeatureNames.Value,
    activity_name: FeatureActivityName.Value,
    attributes: Map[String, String] = Map.empty[String, String] )
