// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.common

import scala.util.matching.Regex

/*
Scrubbers for filtering out information that is prohibited from
logging such as SAS token, etc.

SASScrubber: This one is specifically for scrubbing the Shared Access Signature.
*/

trait Scrubber {
  def scrub(content: String): String
}

object SASScrubber extends Scrubber {
  def scrub(message: String): String = {
    val pattern = new Regex("(?i)sig=[a-z0-9%]{43,63}%3d")
    pattern replaceAllIn(message, "sig=####")
  }
}
