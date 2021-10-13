// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.explainers.split1

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import com.microsoft.azure.synapse.ml.explainers.LeastSquaresRegression
import org.scalactic.{Equality, TolerantNumerics}

class LeastSquaresRegressionSuite extends TestBase {

  implicit val vectorEquality: Equality[BDV[Double]] = breezeVectorEq(1E-3)
  implicit val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(1E-6)

  test("LeastSquaresRegression should regress correctly with weights with intercept.") {
    // Validated with the following code in Jupyter notebook:
    // import numpy as np
    // from sklearn.linear_model import LinearRegression
    // x = np.array([[13, 4, 5], [8, 16, 7], [4, 24, 13], [24, 15, 27], [15, 9, 8]])
    // y = np.array([2, 4, 3, 5, 1])
    // weights = np.array([1, 2, 2, 1, 2])
    // model = LinearRegression(fit_intercept = True).fit(x, y, sample_weight=weights)
    // print(model.intercept_, model.coef_)

    val x = BDM((13d, 4d, 5d), (8d, 16d, 7d), (4d, 24d, 13d), (24d, 15d, 27d), (15d, 9d, 8d))
    val y = BDV[Double](2d, 4d, 3d, 5d, 1d)
    val weights = BDV[Double](1d, 2d, 2d, 1d, 2d)

    val result = new LeastSquaresRegression().fit(x, y, weights, fitIntercept = true)

    assert(result.intercept === 0.6050434048734146)
    assert(result.coefficients(0) === 0.01518619)
    assert(result.coefficients(1) === 0.08135246)
    assert(result.coefficients(2) === 0.082494)
    assert(result.rSquared === 0.43565371571037637)
    assert(result.loss === 8.394650978808151)
  }

  test("LeastSquaresRegression should regress correctly with weights without intercept.") {
    // Validated with the following code in Jupyter notebook:
    // import numpy as np
    // from sklearn.linear_model import LinearRegression
    // x = np.array([[13, 4, 5], [8, 16, 7], [4, 24, 13], [24, 15, 27], [15, 9, 8]])
    // y = np.array([2, 4, 3, 5, 1])
    // weights = np.array([1, 2, 2, 1, 2])
    // model = LinearRegression(fit_intercept = False).fit(x, y, sample_weight=weights)
    // print(model.intercept_, model.coef_)

    val x = BDM((13d, 4d, 5d), (8d, 16d, 7d), (4d, 24d, 13d), (24d, 15d, 27d), (15d, 9d, 8d))
    val y = BDV[Double](2d, 4d, 3d, 5d, 1d)
    val weights = BDV[Double](1d, 2d, 2d, 1d, 2d)

    val result = new LeastSquaresRegression().fit(x, y, weights, fitIntercept = false)

    assert(result.intercept === 0d)
    assert(result.coefficients(0) === 0.05328653)
    assert(result.coefficients(1) === 0.11516329)
    assert(result.coefficients(2) === 0.05274785)
    assert(result.rSquared === 0.43422966666780527)
    assert(result.loss === 8.415833708316397)
  }

  test("LeastSquaresRegression should regress correctly without weights with intercept.") {
    // Validated with the following code in Jupyter notebook:
    // import numpy as np
    // from sklearn.linear_model import LinearRegression
    // x = np.array([[13, 4, 5], [8, 16, 7], [4, 24, 13], [24, 15, 27], [15, 9, 8]])
    // y = np.array([2, 4, 3, 5, 1])
    // model = LinearRegression(fit_intercept = True).fit(x, y)
    // print(model.intercept_, model.coef_)

    val x = BDM((13d, 4d, 5d), (8d, 16d, 7d), (4d, 24d, 13d), (24d, 15d, 27d), (15d, 9d, 8d))
    val y = BDV[Double](2d, 4d, 3d, 5d, 1d)

    val result = new LeastSquaresRegression().fit(x, y, fitIntercept = true)

    assert(result.intercept === 1.3262573957848578)
    assert(result.coefficients(0) === -0.02680322)
    assert(result.coefficients(1) === 0.03347144)
    assert(result.coefficients(2) === 0.13013435)
    assert(result.rSquared === 0.5660246787755755)
    assert(result.loss === 4.339753212244245)
  }

  test("LeastSquaresRegression should regress correctly without weights without intercept.") {
    // Validated with the following code in Jupyter notebook:
    // import numpy as np
    // from sklearn.linear_model import LinearRegression
    // x = np.array([[13, 4, 5], [8, 16, 7], [4, 24, 13], [24, 15, 27], [15, 9, 8]])
    // y = np.array([2, 4, 3, 5, 1])
    // model = LinearRegression(fit_intercept = False).fit(x, y)
    // print(model.intercept_, model.coef_)

    val x = BDM((13d, 4d, 5d), (8d, 16d, 7d), (4d, 24d, 13d), (24d, 15d, 27d), (15d, 9d, 8d))
    val y = BDV[Double](2d, 4d, 3d, 5d, 1d)

    val result = new LeastSquaresRegression().fit(x, y, fitIntercept = false)

    assert(result.intercept === 0d)
    assert(result.coefficients(0) === 0.06309258)
    assert(result.coefficients(1) === 0.10904686)
    assert(result.coefficients(2) === 0.05810592)
    assert(result.rSquared === 0.5579963439586114)
    assert(result.loss === 4.420036560413886)
  }

  ignore("LeastSquaresRegression can solve bigger inputs") {
    // Should take about 1 secs for 10000 * 200 inputs. Ignoring this test to save some time for unit test.
    val (rows, cols) = (10000, 200)
    val x = BDM.rand[Double](rows, cols)
    val y = BDV.rand[Double](rows)
    val w = BDV.rand[Double](rows)

    time {
      new LeastSquaresRegression().fit(x, y, w, fitIntercept = true)
    }
  }
}
