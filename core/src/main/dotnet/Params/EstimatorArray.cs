// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

using System;
using System.Linq;
using System.Reflection;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using MMLSpark.Dotnet.Wrapper;

namespace Microsoft.Spark.ML.Feature.Param
{
    // <summary>
    /// Array of Estimators.
    /// </summary>
    public class EstimatorArray : IJvmObjectReferenceProvider
    {
        private static readonly string s_EstimatorArrayClassName = "org.apache.spark.ml.param.EstimatorArray";

        /// <summary>
        /// Creates a new instance of a <see cref="EstimatorArray"/>
        /// </summary>
        public EstimatorArray() : this(SparkEnvironment.JvmBridge.CallConstructor(s_EstimatorArrayClassName))
        {
        }

        internal EstimatorArray(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        public void AddEstimator<M>(ScalaEstimator<M> value) where M : ScalaModel<M> =>
            Reference.Invoke("addEstimator", value);

        public object GetEstimators() =>
            Reference.Invoke("getEstimators");

    }

}
