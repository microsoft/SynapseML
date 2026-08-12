// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.core.utils

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyModelEquality extends TestBase {

  // These assertions hold both before and after the jaccardSimilarity fix in the companion
  // production PR, so this suite stays independently mergeable. The discriminating
  // partial-overlap tests ship with that fix rather than here.

  test("jaccardSimilarity of identical strings is 1.0") {
    assert(ModelEquality.jaccardSimilarity("hello", "hello") === 1.0)
  }

  test("jaccardSimilarity of strings sharing nothing is 0.0") {
    assert(ModelEquality.jaccardSimilarity("abcd", "wxyz") === 0.0)
  }

  test("jaccardSimilarity is bounded to [0, 1]") {
    Seq(("abc", "def"), ("hello", "hello"), ("kitten", "sitting"), ("a", "ab")).foreach {
      case (s1, s2) =>
        val score = ModelEquality.jaccardSimilarity(s1, s2)
        assert(score >= 0.0 && score <= 1.0, s"$s1 vs $s2 produced $score")
    }
  }

  test("jaccardSimilarity is symmetric") {
    Seq(("abc", "def"), ("kitten", "sitting"), ("hello", "hello")).foreach { case (s1, s2) =>
      assert(ModelEquality.jaccardSimilarity(s1, s2) === ModelEquality.jaccardSimilarity(s2, s1))
    }
  }
}
