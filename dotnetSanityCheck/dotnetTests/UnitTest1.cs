
// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

using System;
using System.IO;
using System.Collections.Generic;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.ML.Feature.Param;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Xunit;
using SynapseMLtest.Utils;
using Synapse.ML.Cognitive;
using Microsoft.Spark.ML;
using Synapse.ML;

namespace SynapseMLtest.Cognitive
{

    public class DetectLastAnomalySuite : IClassFixture<FeaturesFixture>
    {
        public const string TestDataDir = "D:\\repos\\SynapseML\\cognitive\\generated\\test-data\\DetectLastAnomalySuite";
        private readonly SparkSession _spark;
        private readonly IJvmBridge _jvm;
        public DetectLastAnomalySuite(SparkFixture fixture)
        {
            _spark = fixture.Spark;
            _jvm = fixture.Jvm;
        }


        [Fact]
        public void TestDetectLastAnomalyConstructor0()
        {
            void AssertCorrespondence(DetectLastAnomaly model, string name, int num)
            {
                model.Write().Overwrite().Save(Path.Combine(TestDataDir, name));
                _jvm.CallStaticJavaMethod("com.microsoft.ml.spark.core.utils.ModelEquality",
                    "assertEqual", "com.microsoft.ml.spark.cognitive.DetectLastAnomaly", Path.Combine(TestDataDir, name),
                    Path.Combine(TestDataDir, String.Format("model-{0}.model", num)));
            }

            // var complexParamsReader = new ComplexParamsReader<UdfRegistration>();
            // var handlerParam = complexParamsReader.Load(
            //     Path.Combine(TestDataDir, "model-0.model", "complexParams", "handler"));

            // var model = new DetectLastAnomaly()
            //     .SetTimeout(60.0)
            //     .SetHandler(handlerParam)
            //     .SetGranularity("monthly")
            //     .SetUrl("https://westus2.api.cognitive.microsoft.com//anomalydetector/v1.0/timeseries/last/detect")
            //     .SetOutputCol("anomalies")
            //     .SetSeriesCol("inputs")
            //     .SetErrorCol("errors")
            //     .SetConcurrency(1)
            //     .SetSubscriptionKey("4bda24390e4341f89623b7706e7c550f");



            // AssertCorrespondence(IModel, "dotnet-constructor-model-0.model", 0);


        }


    }

}

