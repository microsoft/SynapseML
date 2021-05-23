package com.microsoft.ml.spark.explainers

import breeze.linalg.{norm, DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics.abs
import com.microsoft.ml.spark.core.test.base.TestBase

class LassoRegressionSuite extends TestBase {
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

    assert(norm(result.coefficients - BDV(0.94072731, 0.02135768)) < 1e-3, "coefficients disagreed")
    assert(abs(result.intercept - 0.12830579194328307) < 1e-3, "intercept disagreed")
    assert(abs(result.rSquared - 0.9981453511123433) < 1e-3, "rSquared disagreed")
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

    assert(norm(result.coefficients - BDV(0.99482704, 0.00832043)) < 1e-3, "coefficients disagreed")
    assert(abs(result.intercept - 0d) < 1e-3, "intercept disagreed")
    assert(abs(result.rSquared - 0.9997338898009382) < 1e-3, "rSquared disagreed")
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

    assert(norm(result.coefficients - BDV(0.94674003, 0.01273319)) < 1e-3, "coefficients disagreed")
    assert(abs(result.intercept - 0.10406005674125263) < 1e-3, "intercept disagreed")
    assert(abs(result.rSquared - 0.9980302231860384) < 1e-3, "rSquared disagreed")
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

    assert(norm(result.coefficients - BDV(0.99295345, 0.00510841)) < 1e-3, "coefficients disagreed")
    assert(abs(result.intercept - 0d) < 1e-3, "intercept disagreed")
    assert(abs(result.rSquared - 0.9995342774119687) < 1e-3, "rSquared disagreed")
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
}
