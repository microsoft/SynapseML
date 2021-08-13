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

        internal ParamMap(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        public T Put<T>(Param param, object value) => 
            WrapAsType<T>((JvmObjectReference)Reference.Invoke("put", param, value));

        protected static T WrapAsType<T>(JvmObjectReference reference)
        {
            ConstructorInfo constructor = typeof(T)
                .GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)
                .Single(c =>
                {
                    ParameterInfo[] parameters = c.GetParameters();
                    return (parameters.Length == 1) &&
                        (parameters[0].ParameterType == typeof(JvmObjectReference));
                });

            return (T)constructor.Invoke(new object[] { reference });
        }

    }
}
