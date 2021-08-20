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
    /// Array of ParamMaps
    /// </summary>
    public class ArrayParamMap : IJvmObjectReferenceProvider
    {
        private static readonly string s_ArrayParamMapClassName = "org.apache.spark.ml.param.ArrayParamMap";

        /// <summary>
        /// Creates a new instance of a <see cref="ArrayParamMap"/>
        /// </summary>
        public ArrayParamMap() : this(SparkEnvironment.JvmBridge.CallConstructor(s_ArrayParamMapClassName))
        {
        }

        internal ArrayParamMap(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        public void AddParamMap(ParamMap value) =>
            Reference.Invoke("addParamMap", value);
        
        public object GetParamMaps() =>
            Reference.Invoke("getParamMaps");

    }
    
}
