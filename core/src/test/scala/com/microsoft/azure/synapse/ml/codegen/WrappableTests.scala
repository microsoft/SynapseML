// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.codegen

import com.microsoft.azure.synapse.ml.core.test.base.TestBase
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.{RegressionModel, Regressor}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset

class WrappableTests extends TestBase {

    class TestRegressorModel()
      extends RegressionModel[Vector, TestRegressorModel] {
        override def predict(features: Vector): Double = 0.0

        override def copy(extra: ParamMap): TestRegressorModel = defaultCopy(extra)

        override val uid: String = "test"
    }

    object TestRegressor
    class TestRegressor()
      extends Regressor[Vector, TestRegressor, TestRegressorModel]
        with Wrappable {
        override def copy(extra: ParamMap): TestRegressor = defaultCopy(extra)

        override protected def train(dataset: Dataset[_]): TestRegressorModel = {
            new TestRegressorModel()
        }

        def getCompanionModelClassName(): String = {
            this.companionModelClassName
        }

        override val uid: String = "test"
    }

    test ("test CompanionModelClassName") {
        val regressorCompanionModelClasName = new TestRegressor().getCompanionModelClassName
        assert(regressorCompanionModelClasName.equals(
            "com.microsoft.azure.synapse.ml.codegen.WrappableTests.TestRegressorModel"))
    }
}
