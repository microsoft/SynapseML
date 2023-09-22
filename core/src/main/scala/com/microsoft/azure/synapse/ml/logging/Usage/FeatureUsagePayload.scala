// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.Usage

case class FeatureUsagePayload(feature_name: String,
    activity_name: String,
    attributes: Map[String, String])
