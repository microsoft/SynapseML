// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.sql.internal

import com.fasterxml.jackson.core.JsonFactory
import org.apache.spark.sql.catalyst.json.JSONOptions

object SQLConfInjections {
  def jsonFactory(options: Map[String, String]): JsonFactory =
    new JSONOptions(options, SQLConf.get.sessionLocalTimeZone).buildJsonFactory()
}
