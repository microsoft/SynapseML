// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.scalactic.Tolerance.convertNumericToPlusOrMinusWrapper

class VerifyKahanSum extends TestBase {
  private val large = Math.pow(2, 50)
  private val small = Math.pow(0.5, 15)

  test ("Verify KahanBabushkaNeumaierSum simple") {
      var s = KahanSum()

      assert (s.toDouble == 0)

      s = s + 1.0
      assert (s.toDouble == 1)

      s = s + 2.0
      assert (s.toDouble == 3)
    }

  test ("Verify KahanBabushkaNeumaierSum better than naive") {
    var compensatedSum = KahanSum()
    var naiveSum = 0.0

    naiveSum += large
    compensatedSum += large

    for (_ <- 0 to Math.pow(2, 15).toInt) {
      compensatedSum += small
      naiveSum += small
    }

    val expected = large + 1

    assert (Math.abs(expected - naiveSum) > Math.abs(expected - compensatedSum.toDouble))
  }

  test ("Verify KahanBabushkaNeumaierSum incremental") {
    var first = KahanSum()
    var second = KahanSum()

    first = first + large
    second = second + large

    for (_ <- 0 to Math.pow(2, 15).toInt)
      first = first + small
      second = second + small

    var firstPlusSecond = first + second
    var expected = 2 * large + 2

    assert (firstPlusSecond.toDouble === expected +- 1)

    firstPlusSecond += large
    for (_ <- 0 to Math.pow(2, 15).toInt)
      firstPlusSecond += small

    expected = 3 * large + 3
    assert (firstPlusSecond.toDouble === expected +- 1)
  }
}
