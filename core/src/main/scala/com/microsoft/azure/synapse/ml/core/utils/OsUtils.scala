// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

object OsUtils {
  val IsWindows: Boolean = System.getProperty("os.name").toLowerCase().indexOf("win") >= 0
}
