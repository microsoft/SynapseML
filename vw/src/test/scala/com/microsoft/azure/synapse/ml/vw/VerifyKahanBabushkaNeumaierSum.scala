package com.microsoft.azure.synapse.ml.vw

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.scalactic.{Equality, TolerantNumerics}

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

//  test ("Verify KahanBabushkaNeumaierSum better than naive") {
//    var compensated_sum = KahanBabushkaNeumaierSum()
//    var naive_sum = 0.0
//
//    naive_sum += large
//    compensated_sum += large
//
//    for (_ <- 0 to Math.pow(2, 15).toInt) {
//      compensated_sum += small
//      naive_sum += small
//    }
//
//    val expected = large + 1
//    println(s"naive: $naive_sum")
//    println(s"kahan: ${compensated_sum.toDouble}")
//    assert (Math.abs(expected - naive_sum) > Math.abs(expected - compensated_sum.toDouble))
//  }
//
//  test ("Verify KahanBabushkaNeumaierSum incremental") {
//    var first = KahanBabushkaNeumaierSum()
//    var second = KahanBabushkaNeumaierSum()
//
//    first = first + large
//    second = second + large
//
//    for (_ <- 0 to Math.pow(2, 15).toInt)
//      first = first + small
//      second = second + small
//
//    val firstPlusSecond = first + second
//    val expected = 2 * large + 2
//
//    implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1e-2)
//
////    2.251799813685249E15
////    2.25179981368525E15
//    println(firstPlusSecond.toDouble)
//    println(expected)
//
//    // TODO: not sure why this fails?
//    assert (doubleEquality.areEqual(firstPlusSecond.toDouble, expected))
//    // assert (firstPlusSecond.toDouble == expected)
//    // assert ( math.isclose(float(first_plus_second), expected, rel_tol = 0.5**52)
//
//    //
//    //  first_plus_second += large
//    //  for _ in range(2 ** 15):
//    //    first_plus_second += small
//    //
//    //  expected = 3 * large + 3
//    //  assert math.isclose(float(first_plus_second), expected, rel_tol = 0.5**52)
//  }
}
