// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage

case class FeatureUsagePayload(feature_name: UsageFeatureNames.Value,
    activity_name: FeatureActivityName.Value,
    attributes: Map[String, String] = Map.empty[String, String] )
