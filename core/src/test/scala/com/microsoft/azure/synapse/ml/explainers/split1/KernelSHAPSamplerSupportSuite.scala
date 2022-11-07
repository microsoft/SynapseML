// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers.split1

import breeze.linalg.sum
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.explainers.KernelSHAPSamplerSupport
import org.scalatest.matchers.should.Matchers._

class KernelSHAPSamplerSupportSuite extends TestBase {

  test("generateCoalitions should generate correct coalitions") {
    an[AssertionError] shouldBe thrownBy {
      val sampler = new KernelSHAPSamplerSupport {
        override protected def featureSize: Int = 0

        override protected def numSamples: Int = 1000

        override protected def infWeight: Double = 1E8
      }

      sampler.generateCoalitions
    }

    an[AssertionError] shouldBe thrownBy {
      val sampler = new KernelSHAPSamplerSupport {
        override protected def featureSize: Int = 5

        override protected def numSamples: Int = -1

        override protected def infWeight: Double = 1E8
      }

      sampler.generateCoalitions
    }

    val sampler1 = new KernelSHAPSamplerSupport {
      override protected def featureSize: Int = 5

      override protected def numSamples: Int = 32

      override protected def infWeight: Double = 1E8
    }

    val (c1, w1) = sampler1.generateCoalitions.toList.unzip
    c1.length shouldBe 32
    w1.head shouldBe 1E8
    w1(1) shouldBe 1E8
    c1.count(coal => sum(coal) == 0) shouldBe 1
    c1.count(coal => sum(coal) == 5) shouldBe 1
    c1.count(coal => sum(coal) == 1) shouldBe 5
    c1.count(coal => sum(coal) == 4) shouldBe 5
    c1.count(coal => sum(coal) == 2) shouldBe 10
    c1.count(coal => sum(coal) == 3) shouldBe 10

    val sampler2 = new KernelSHAPSamplerSupport {
      override protected def featureSize: Int = 500

      override protected def numSamples: Int = 4

      override protected def infWeight: Double = 1E8
    }

    val (c2, w2) = sampler2.generateCoalitions.toList.unzip
    c2.length shouldBe 4
    w2 shouldBe List(1E8, 1E8, 1.0, 1.0)
    c2.count(coal => sum(coal) == 0) shouldBe 1
    c2.count(coal => sum(coal) == 500) shouldBe 1
    c2.count(coal => sum(coal) == 1) shouldBe 1
    c2.count(coal => sum(coal) == 2) shouldBe 1

    val sampler3 = new KernelSHAPSamplerSupport {
      override protected def featureSize: Int = 500

      override protected def numSamples: Int = 1000

      override protected def infWeight: Double = 1E8
    }

    val (c3, w3) = sampler3.generateCoalitions.toList.unzip
    c3.length shouldBe 1000
    w3.take(2).foreach(_ shouldBe 1E8)
    w3.drop(2).foreach(_ shouldBe 1d)
    c3.count(coal => sum(coal) == 0) shouldBe 1
    c3.count(coal => sum(coal) == 500) shouldBe 1
    c3.count(coal => sum(coal) == 1) shouldBe 74
    c3.count(coal => sum(coal) == 499) shouldBe 74
    c3.count(coal => sum(coal) == 2) shouldBe 37
    c3.count(coal => sum(coal) == 498) shouldBe 37
  }
}
