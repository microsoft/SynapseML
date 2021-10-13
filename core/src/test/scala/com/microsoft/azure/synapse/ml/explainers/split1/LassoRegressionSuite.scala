// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers.split1

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.explainers.LassoRegression
import org.scalactic.{Equality, TolerantNumerics}

class LassoRegressionSuite extends TestBase {
  implicit val vectorEquality: Equality[BDV[Double]] = breezeVectorEq(1E-3)
  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1E-3)

  // The following test can be verified in python with following code:
  // from sklearn import linear_model
  // X = np.array([[1, 1], [2, -2], [3, 3], [4, 5]])
  // y = np.array([1.01, 1.98, 3.03, 4.05])
  // clf = linear_model.Lasso(alpha=0.05, fit_intercept = True)
  // clf.fit(X, y)
  // print(clf.coef_)
  // print(clf.intercept_)
  test("LassoRegression should regress correctly without weights with intercept") {
    val a = BDM((1.0, 1.0), (2.0, -2.0), (3.0, 3.0), (4.0, 5.0))
    val b = BDV(1.01, 1.98, 3.03, 4.05)

    val result = new LassoRegression(0.05d).fit(a, b, fitIntercept = true)
    assert(result.coefficients === BDV(0.94072731, 0.02135768), "coefficients disagreed")
    assert(result.intercept === 0.12830579194328307, "intercept disagreed")
    assert(result.rSquared === 0.9981453805277254, "rSquared disagreed")
    assert(result.loss === 0.05769498881462409, "loss disagreed")
  }

  // The following test can be verified in python with following code:
  // from sklearn import linear_model
  // X = np.array([[1, 1], [2, -2], [3, 3], [4, 5]])
  // y = np.array([1.01, 1.98, 3.03, 4.05])
  // clf = linear_model.Lasso(alpha=0.05, fit_intercept = False)
  // clf.fit(X, y)
  // print(clf.coef_)
  // print(clf.intercept_)
  test("LassoRegression should regress correctly without weights without intercept") {
    val a = BDM((1.0, 1.0), (2.0, -2.0), (3.0, 3.0), (4.0, 5.0))
    val b = BDV(1.01, 1.98, 3.03, 4.05)
    val result = new LassoRegression(0.05d).fit(a, b, fitIntercept = false)

    assert(result.coefficients === BDV(0.99482704, 0.00832043), "coefficients disagreed")
    assert(result.intercept === 0d, "intercept disagreed")
    assert(result.rSquared === 0.9997338898009382, "rSquared disagreed")
    assert(result.loss === 0.05153237409988701, "loss disagreed")
  }

  // The following test can be verified in python with following code:
  // from sklearn import linear_model
  // X = np.array([[1, 1], [2, -2], [3, 3], [4, 5]])
  // y = np.array([1.01, 1.98, 3.03, 4.05])
  // w = np.array([4, 3, 2, 1])
  // clf = linear_model.Lasso(alpha=0.05, fit_intercept = True)
  // clf.fit(X, y, sample_weight = w)
  // print(clf.coef_)
  // print(clf.intercept_)
  test("LassoRegression should regress correctly with weights with intercept") {
    val a = BDM((1.0, 1.0), (2.0, -2.0), (3.0, 3.0), (4.0, 5.0))
    val b = BDV(1.01, 1.98, 3.03, 4.05)
    val weights = BDV(4d, 3d, 2d, 1d)
    val result = new LassoRegression(0.05d).fit(a, b, weights, fitIntercept = true)

    assert(result.coefficients === BDV(0.94674003, 0.01273319), "coefficients disagreed")
    assert(result.intercept === 0.10406005674125263, "intercept disagreed")
    assert(result.rSquared === 0.9975330780923556, "rSquared disagreed")
    assert(result.loss === 0.0732464464883231, "loss disagreed")
  }

  // The following test can be verified in python with following code:
  // from sklearn import linear_model
  // X = np.array([[1, 1], [2, -2], [3, 3], [4, 5]])
  // y = np.array([1.01, 1.98, 3.03, 4.05])
  // w = np.array([4, 3, 2, 1])
  // clf = linear_model.Lasso(alpha=0.05, fit_intercept = False)
  // clf.fit(X, y, sample_weight = w)
  // print(clf.coef_)
  // print(clf.intercept_)
  test("LassoRegression should regress correctly with weights without intercept") {
    val a = BDM((1.0, 1.0), (2.0, -2.0), (3.0, 3.0), (4.0, 5.0))
    val b = BDV(1.01, 1.98, 3.03, 4.05)
    val weights = BDV(4d, 3d, 2d, 1d)
    val result = new LassoRegression(0.05d).fit(a, b, weights, fitIntercept = false)

    assert(result.coefficients === BDV(0.99295345, 0.00510841), "coefficients disagreed")
    assert(result.intercept === 0d, "intercept disagreed")
    assert(result.rSquared === 0.9994167353137898, "rSquared disagreed")
    assert(result.loss === 0.055878038756038625, "loss disagreed")
  }

  ignore("LassoRegression can solve bigger inputs") {
    // Should take about 6 secs for 10000 * 200 inputs. Ignoring this test to save some time for unit test.
    val (rows, cols) = (10000, 200)
    val x = BDM.rand[Double](rows, cols)
    val y = BDV.rand[Double](rows)
    val w = BDV.rand[Double](rows)

    time {
      new LassoRegression(0.05d).fit(x, y, w, fitIntercept = true)
    }
  }

  test("Lasso regression with single value variable") {
    val a = BDM((1.0, 1.0), (2.0, 1.0), (3.0, 1.0), (4.0, 1.0))
    val b = BDV(2.0, 3.0, 4.0, 5.0)
    val result = new LassoRegression(0d).fit(a, b, fitIntercept = true)
    assert(result.coefficients === BDV(1.0, 0), "coefficients disagreed")
    assert(result.intercept === 1d, "intercept disagreed")
    assert(result.rSquared === 1d, "rSquared disagreed")
    assert(result.loss === 0d, "loss disagreed")
  }
}
