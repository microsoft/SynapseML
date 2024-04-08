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

    test ("test CompanionModelClassName") {
        val regressorCompanionModelClasName = new TestRegressor().getCompanionModelClassName
        assert(regressorCompanionModelClasName.equals(
            "com.microsoft.azure.synapse.ml.codegen.WrappableTests.TestRegressorModel"))
    }
}
