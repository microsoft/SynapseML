// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.sql.internal

import com.fasterxml.jackson.core.JsonFactory
import org.apache.spark.sql.catalyst.json.JSONOptions

import scala.collection.JavaConverters._

object SQLConfInjections {
  // Enumerating values would also evaluate unrelated optional Hive defaults.
  def registeredKeys: Seq[String] = SQLConf.getConfigEntries().asScala.iterator.map(_.key).toVector

  def jsonFactory(options: Map[String, String]): JsonFactory =
    new JSONOptions(options, SQLConf.get.sessionLocalTimeZone).buildJsonFactory()
}
