// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.logging.common

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyScrubber extends TestBase {

  test("SASScrubber scrubs SAS signature from URL") {
    // SAS tokens typically contain sig= followed by base64-like encoded signature
    // Dummy URL for testing - not a real endpoint
    // scalastyle:off line.size.limit
    val urlWithSas = "https://storage.blob.core.windows.net/container/file?sig=abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG%3d"
    // scalastyle:on line.size.limit
    val result = SASScrubber.scrub(urlWithSas)
    assert(result.contains("sig=####"))
    assert(!result.contains("abcdefghijklmnopqrstuvwxyz"))
  }

  test("SASScrubber handles multiple SAS signatures in one string") {
    // Use signatures that match the pattern: sig= followed by 43-63 alphanumeric/% chars ending with %3d
    val sig1 = "sig=abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG%3d"
    val sig2 = "sig=XYZ987wvu654tsr321qpo098nml765kji432hgf109AB%3d"
    val message = s"URL1: $sig1 and URL2: $sig2"
    val result = SASScrubber.scrub(message)
    // Both signatures should be replaced
    assert(!result.contains("abcdefghijklmnopqrstuvwxyz"))
    assert(!result.contains("XYZ987wvu654"))
    assert(result.contains("sig=####"))
  }

  test("SASScrubber leaves non-SAS content unchanged") {
    val message = "This is a normal log message without any signatures"
    val result = SASScrubber.scrub(message)
    assert(result === message)
  }

  test("SASScrubber is case insensitive for sig parameter") {
    val lowerCase = "https://test.com?sig=abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG%3d"
    val upperCase = "https://test.com?SIG=abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG%3d"
    val mixedCase = "https://test.com?SiG=abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG%3d"

    assert(SASScrubber.scrub(lowerCase).contains("sig=####"))
    assert(SASScrubber.scrub(upperCase).contains("sig=####"))
    assert(SASScrubber.scrub(mixedCase).contains("sig=####"))
  }

  test("SASScrubber handles empty string") {
    assert(SASScrubber.scrub("") === "")
  }

  test("SASScrubber handles string with only sig= but invalid signature") {
    // Too short signature should not be scrubbed
    val shortSig = "https://test.com?sig=abc"
    assert(SASScrubber.scrub(shortSig) === shortSig)
  }

  test("SASScrubber preserves text before and after signature") {
    val message = "Prefix text sig=abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG%3d suffix text"
    val result = SASScrubber.scrub(message)
    assert(result.startsWith("Prefix text"))
    assert(result.endsWith("suffix text"))
    assert(result.contains("sig=####"))
  }

  test("SASScrubber implements Scrubber trait") {
    val scrubber: Scrubber = SASScrubber
    val message = "Test sig=abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG%3d"
    val result = scrubber.scrub(message)
    assert(result.contains("sig=####"))
  }
}
