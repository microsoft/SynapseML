package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.Matchers.convertNumericToPlusOrMinusWrapper

class VerifyKahanBabushkaNeumaierSum extends TestBase {
  val large = Math.pow(2, 50)
  val small = Math.pow(0.5, 15)

  test ("Verify KahanBabushkaNeumaierSum simple") {

      var s = KahanSum()

      assert (s.toDouble() == 0)

      s = s + 1.0
      assert (s.toDouble() == 1)

      s = s + 2.0
      assert (s.toDouble() == 3)
    }

  test ("Verify KahanBabushkaNeumaierSum better than naive") {
    var compensated_sum = KahanSum()
    var naive_sum = 0.0

    naive_sum += large
    compensated_sum += large

    for (_ <- 0 to Math.pow(2, 15).toInt) {
      compensated_sum += small
      naive_sum += small
    }

    val expected = large + 1

//    println(s"naive: ${Math.abs(expected - naive_sum)}")
//    println(s"kahan: ${Math.abs(expected - compensated_sum.toDouble)}")

    assert (Math.abs(expected - naive_sum) > Math.abs(expected - compensated_sum.toDouble))
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

    // abs vs rel tolerance: https://github.com/VowpalWabbit/estimators/blob/03c8ba619d68f54849d4fa2da2b1a148e6cdb990/estimators/test/test_math.py#L49
    assert (firstPlusSecond.toDouble === expected +- 1)

    firstPlusSecond += large
    for (_ <- 0 to Math.pow(2, 15).toInt)
      firstPlusSecond += small

    expected = 3 * large + 3
    assert (firstPlusSecond.toDouble === expected +- 1)
  }
}
