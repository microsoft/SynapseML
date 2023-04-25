// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.common

import scala.util.matching.Regex

trait Scrubber {
  def scrub(content: String): String
}

object SASScrubber extends Scrubber {
  def scrub(message: String): String = {
    val pattern = new Regex("(?i)sig=[a-z0-9%]{43,63}%3d")
    pattern replaceAllIn(message, "sig=####")
  }
}
