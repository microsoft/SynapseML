// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage

case class FeatureUsagePayload(feature_name: UsageFeatureName,
    activity_name: FeatureActivityName,
    attributes: Map[String, String])

abstract class UsageFeatureName{
  def getFeatureName: String
}

class FeatureSynapseML extends UsageFeatureName {
  override def getFeatureName: String = "SynapseML"
}

abstract class FeatureActivityName{
  def getFeatureActivityName: String = "Invalid"
}

class FeatureActivityFit extends FeatureActivityName{
  override def getFeatureActivityName: String = "Fit"
}

class FeatureActivityTransform extends FeatureActivityName{
  override def getFeatureActivityName: String = "Transform"
}

class FeatureActivityTrain extends FeatureActivityName{
  override def getFeatureActivityName: String = "Train"
}

class FeatureActivityInvalid extends FeatureActivityName{
  override def getFeatureActivityName: String = "Invalid"
}
