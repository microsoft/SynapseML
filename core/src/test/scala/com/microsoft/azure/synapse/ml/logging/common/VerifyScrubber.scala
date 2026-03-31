// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.common

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyScrubber extends TestBase {

  test("SASScrubber replaces SAS signature in URL") {
    val url = "https://storage.blob.core.windows.net/container?sv=2021-06-08" +
      "&sig=abcdef1234567890abcdef1234567890abcdef12345%3d&se=2024-01-01"
    val scrubbed = SASScrubber.scrub(url)
    assert(scrubbed.contains("sig=####"))
    assert(!scrubbed.contains("abcdef1234567890"))
  }

  test("SASScrubber leaves strings without SAS tokens unchanged") {
    val message = "This is a normal log message with no SAS token"
    assert(SASScrubber.scrub(message) === message)
  }

  test("SASScrubber handles multiple SAS tokens in same string") {
    val url1 = "https://storage1.blob.core.windows.net/c1?sig=abcdef1234567890abcdef1234567890abcdef12345%3d"
    val url2 = "https://storage2.blob.core.windows.net/c2?sig=123456abcdef7890123456abcdef7890123456abc78%3d"
    val combined = s"$url1 and $url2"
    val scrubbed = SASScrubber.scrub(combined)
    assert(scrubbed === "https://storage1.blob.core.windows.net/c1?sig=####" +
      " and https://storage2.blob.core.windows.net/c2?sig=####")
  }

  test("SASScrubber is case insensitive") {
    val upperUrl = "https://storage.blob.core.windows.net/container" +
      "?SIG=ABCDEF1234567890ABCDEF1234567890ABCDEF12345%3D&se=2024-01-01"
    val mixedUrl = "https://storage.blob.core.windows.net/container" +
      "?Sig=AbCdEf1234567890AbCdEf1234567890AbCdEf12345%3d&se=2024-01-01"
    val scrubbedUpper = SASScrubber.scrub(upperUrl)
    val scrubbedMixed = SASScrubber.scrub(mixedUrl)
    assert(scrubbedUpper.contains("sig=####"))
    assert(scrubbedMixed.contains("sig=####"))
    assert(!scrubbedUpper.contains("ABCDEF1234567890"))
    assert(!scrubbedMixed.contains("AbCdEf1234567890"))
  }

  test("SASScrubber preserves rest of URL around the signature") {
    val url = "https://storage.blob.core.windows.net/container?sv=2021-06-08" +
      "&sig=abcdef1234567890abcdef1234567890abcdef12345%3d&se=2024-01-01&sp=r"
    val scrubbed = SASScrubber.scrub(url)
    assert(scrubbed.contains("sv=2021-06-08"))
    assert(scrubbed.contains("se=2024-01-01"))
    assert(scrubbed.contains("sp=r"))
    assert(scrubbed.contains("sig=####"))
    assert(scrubbed === "https://storage.blob.core.windows.net/container?sv=2021-06-08" +
      "&sig=####&se=2024-01-01&sp=r")
  }
}
