// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using MMLSpark.Dotnet.Wrapper;

namespace Microsoft.Spark.ML.Feature.Param
{

    // <summary>
    /// A param and its value.
    /// </summary>
    public sealed class ParamPair<T> : IJvmObjectReferenceProvider
    {
        private static readonly string s_ParamPairClassName = "org.apache.spark.ml.param.ParamPair";

        /// <summary>
        /// Creates a new instance of a <see cref="ParamPair"/>
        /// </summary>
        public ParamPair(Param param, T value)
            : this(SparkEnvironment.JvmBridge.CallConstructor(s_ParamPairClassName, param, value))
        {
        }

        internal ParamPair(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }
    }

    // <summary>
    /// Param for Estimator.  Needed as spark has explicit 
    /// com.microsoft.ml.spark.core.serialize.params for many different types but not Estimator.
    /// </summary>
    public class ParamMap : IJvmObjectReferenceProvider
    {
        private static readonly string s_ParamMapClassName = "org.apache.spark.ml.param.ParamMap";

        /// <summary>
        /// Creates a new instance of a <see cref="ParamMap"/>
        /// </summary>
        public ParamMap() : this(SparkEnvironment.JvmBridge.CallConstructor(s_ParamMapClassName))
        {
        }

        public ParamMap(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        public ParamMap Put(Param param, object value) =>
            WrapAsParamMap((JvmObjectReference)Reference.Invoke("put", param, value));

        public override string ToString() =>
            (string)Reference.Invoke("toString");


        private static ParamMap WrapAsParamMap(object obj) =>
            new ParamMap((JvmObjectReference)obj);

    }

    // <summary>
    /// Represents the parameter values.
    /// </summary>
    public abstract class ParamSpace
    {
        public abstract IEnumerable<ParamMap> ParamMaps();
    }

}
