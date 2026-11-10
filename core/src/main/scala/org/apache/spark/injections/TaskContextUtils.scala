// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package org.apache.spark.injections

import org.apache.spark.TaskContext

object TaskContextUtils {
  def getKillReason(context: TaskContext): Option[String] = context.getKillReason()
}
