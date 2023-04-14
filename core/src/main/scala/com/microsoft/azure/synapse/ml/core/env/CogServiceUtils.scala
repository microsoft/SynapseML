// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.env

// Set environment variables for AADToken and Urls for cognitive services
object CogServiceUtils {

  val AADToken: String = sys.env.getOrElse("AADTOKEN", "")

  val BaseEndpoint: String = sys.env.getOrElse("ML_WORKLOAD_ENDPOINT", "")
}
