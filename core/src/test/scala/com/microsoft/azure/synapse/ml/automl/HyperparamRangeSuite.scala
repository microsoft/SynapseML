// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.automl

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

// scalastyle:off magic.number
/** Covers the bounds and seeding contract shared by every RangeHyperParam. */
class HyperparamRangeSuite extends TestBase {

  private val draws = 500

  test("LongRangeHyperParam stays within its range") {
    val hp = new LongRangeHyperParam(0L, 100L, seed = 42)
    val values = (1 to draws).map(_ => hp.getNext())
    assert(values.forall(v => v >= 0L && v < 100L), s"out of range: ${values.filter(v => v < 0L || v >= 100L)}")
    assert(values.toSet.size > 1)
  }

  test("LongRangeHyperParam stays within a range too large to fit in an Int") {
    val min = -4000000000L
    val max = 4000000000L
    val hp = new LongRangeHyperParam(min, max, seed = 7)
    val values = (1 to draws).map(_ => hp.getNext())
    assert(values.forall(v => v >= min && v < max))
    assert(values.exists(_ < 0L) && values.exists(_ > 0L))
  }

  test("LongRangeHyperParam with an empty range returns min") {
    val hp = new LongRangeHyperParam(5L, 5L, seed = 42)
    assert((1 to 10).forall(_ => hp.getNext() === 5L))
  }

  test("LongRangeHyperParam covers a power-of-two range without bias") {
    // Power-of-two bounds take the mask branch; every bucket must still be reachable.
    val hp = new LongRangeHyperParam(0L, 8L, seed = 3)
    val counts = (1 to 4000).map(_ => hp.getNext()).groupBy(identity).map { case (k, v) => k -> v.size }
    assert(counts.keySet === (0L until 8L).toSet)
    assert(counts.values.forall(c => c > 300 && c < 700), s"uneven distribution: $counts")
  }

  test("LongRangeHyperParam covers a non-power-of-two range without bias") {
    // Non-power-of-two bounds take the rejection branch, which must terminate and stay uniform.
    val hp = new LongRangeHyperParam(0L, 7L, seed = 3)
    val counts = (1 to 4000).map(_ => hp.getNext()).groupBy(identity).map { case (k, v) => k -> v.size }
    assert(counts.keySet === (0L until 7L).toSet)
    assert(counts.values.forall(c => c > 350 && c < 800), s"uneven distribution: $counts")
  }

  test("IntRangeHyperParam stays within its range") {
    val hp = new IntRangeHyperParam(5, 15, seed = 42)
    val values = (1 to draws).map(_ => hp.getNext())
    assert(values.forall(v => v >= 5 && v < 15))
  }

  test("DoubleRangeHyperParam stays within its range") {
    val hp = new DoubleRangeHyperParam(0.0, 1.0, seed = 42)
    assert((1 to draws).map(_ => hp.getNext()).forall(v => v >= 0.0 && v < 1.0))
  }

  test("FloatRangeHyperParam stays within its range") {
    val hp = new FloatRangeHyperParam(0.0f, 1.0f, seed = 42)
    assert((1 to draws).map(_ => hp.getNext()).forall(v => v >= 0.0f && v < 1.0f))
  }

  test("equal seeds reproduce equal sequences and differing seeds diverge") {
    def draw(hp: Dist[_]): Seq[Any] = (1 to 20).map(_ => hp.getNext)

    assert(draw(new IntRangeHyperParam(0, 1000, 11)) === draw(new IntRangeHyperParam(0, 1000, 11)))
    assert(draw(new LongRangeHyperParam(0L, 1000L, 11)) === draw(new LongRangeHyperParam(0L, 1000L, 11)))
    assert(draw(new DoubleRangeHyperParam(0.0, 1.0, 11)) === draw(new DoubleRangeHyperParam(0.0, 1.0, 11)))
    // FloatRangeHyperParam delegates to an inner DoubleRangeHyperParam; it must forward its seed.
    assert(draw(new FloatRangeHyperParam(0.0f, 1.0f, 11)) === draw(new FloatRangeHyperParam(0.0f, 1.0f, 11)))
    assert(draw(new FloatRangeHyperParam(0.0f, 1.0f, 11)) !== draw(new FloatRangeHyperParam(0.0f, 1.0f, 12)))
  }

  test("HyperParamUtils.getRangeHyperParam honors the seed for every numeric type") {
    Seq[(Any, Any)]((0, 1000), (0L, 1000L), (0.0, 1.0), (0.0f, 1.0f)).foreach { case (min, max) =>
      val a = HyperParamUtils.getRangeHyperParam(min, max, 99)
      val b = HyperParamUtils.getRangeHyperParam(min, max, 99)
      assert((1 to 20).map(_ => a.getNext) === (1 to 20).map(_ => b.getNext), s"seed ignored for $min/$max")
    }
  }
}
// scalastyle:on magic.number
