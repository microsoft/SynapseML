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
    /// Array of Transformers.
    /// </summary>
    public class TransformerArray : IJvmObjectReferenceProvider
    {
        private static readonly string s_TransformerArrayClassName = "org.apache.spark.ml.param.TransformerArray";

        /// <summary>
        /// Creates a new instance of a <see cref="TransformerArray"/>
        /// </summary>
        public TransformerArray() : this(SparkEnvironment.JvmBridge.CallConstructor(s_TransformerArrayClassName))
        {
        }

        internal TransformerArray(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; init; }

        public void AddTransformer(ScalaTransformer value) =>
            Reference.Invoke("addTransformer", value);

        public object GetTransformers() =>
            Reference.Invoke("getTransformers");

    }
    
}
