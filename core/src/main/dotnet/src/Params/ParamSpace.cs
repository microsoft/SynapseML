// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

using System.Collections.Generic;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.ML.Feature.Param;

namespace Synapse.ML.Automl
{

    public abstract class DistObject
    {
    }

    // <summary>
    /// Represents a distribution of values.
    /// </summary>
    /// <param name="T">
    /// The type T of the values generated.
    /// </param>
    public abstract class Dist<T> : DistObject
    {
        public abstract T GetNext();

        public abstract ParamPair<T> GetParamPair(Param param);

    }


    // <summary>
    /// Represents a generator of parameters with specified distributions added by the HyperparamBuilder.
    /// </summary>
    public class RandomSpace : ParamSpace, IJvmObjectReferenceProvider
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.automl.RandomSpace";

        /// <summary>
        /// Creates a new instance of a <see cref="RandomSpace"/>
        /// </summary>
        public RandomSpace((Param, DistObject)[] value) 
            : this(SparkEnvironment.JvmBridge.CallConstructor(s_className, value))
        {
        }

        internal RandomSpace(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; init; }

        override public IEnumerable<ParamMap> ParamMaps() =>
            (IEnumerable<ParamMap>)Reference.Invoke("paramMaps");
    }
    
}
