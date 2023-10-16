// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.lightgbm

import scala.collection.mutable
import scala.language.existentials

/** Class for converting column values to group ID.
  *
  * Ints can just be returned, but a map of Long and String values is maintained so that unique and
  * consistent values can be returned.
  */
class GroupIdManager {
  private val stringGroupIds = mutable.Map[String, Int]()
  private val longGroupIds = mutable.Map[Long, Int]()
  private[this] val lock = new Object()

  /** Convert a group ID into a unique Int.
    *
    * @param groupValue The original group ID value
    */
  def getUniqueIdForGroup(groupValue: Any): Int = {
    groupValue match {
      case iVal: Int =>
        iVal // If it's already an Int, just return
      case lVal: Long =>
        lock.synchronized {
          if (!longGroupIds.contains(lVal)) {
            longGroupIds(lVal) = longGroupIds.size
          }
          longGroupIds(lVal)
        }
      case sVal: String =>
        lock.synchronized {
          if (!stringGroupIds.contains(sVal)) {
            stringGroupIds(sVal) = longGroupIds.size
          }
          stringGroupIds(sVal)
        }
      case _ =>
        throw new IllegalArgumentException(s"Unsupported group column type: '${groupValue.getClass.getName}'")
    }
  }
}
