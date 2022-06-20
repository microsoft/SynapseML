// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.ML.Feature.Param;
using Microsoft.Spark.Utils;

namespace Synapse.ML.Automl
{
    // <summary>
    /// Specifies the search space for hyperparameters.
    /// </summary>
    public class HyperparamBuilder : IJvmObjectReferenceProvider
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.automl.HyperparamBuilder";

        /// <summary>
        /// Creates a new instance of a <see cref="HyperparamBuilder"/>
        /// </summary>
        public HyperparamBuilder()
            : this(SparkEnvironment.JvmBridge.CallConstructor(s_className))
        {
        }

        internal HyperparamBuilder(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; init; }

        public HyperparamBuilder AddHyperparam<T>(Param param, Dist<T> values) =>
            WrapAsHyperparamBuilder((JvmObjectReference)Reference.Invoke("addHyperparam", param, values));

        public (Param, DistObject)[] Build() {
            JvmObjectReference[] jvmObjects = (JvmObjectReference[])Reference.Invoke("build");
            var result = new (Param, DistObject)[jvmObjects.Length];
            Dictionary<string, Type> classMapping = JvmObjectUtils.ConstructJavaClassMapping(
                typeof(DistObject),
                "s_className");
            for (int i = 0; i < jvmObjects.Length; i++)
            {   
                Param param = new Param((JvmObjectReference)jvmObjects[i].Invoke("_1"));
                JvmObjectReference distObject = (JvmObjectReference)jvmObjects[i].Invoke("_2");
                if (JvmObjectUtils.TryConstructInstanceFromJvmObject(
                    distObject,
                    classMapping,
                    out DistObject instance))
                {
                    result[i] = (param, instance);
                }
            }
            return result;
        }

        private static HyperparamBuilder WrapAsHyperparamBuilder(object obj) =>
            new HyperparamBuilder((JvmObjectReference)obj);

    }

    public abstract class RangeHyperParam<T> : Dist<T>, IJvmObjectReferenceProvider
    {
        public RangeHyperParam(string className, T min, T max, long seed)
            : this(SparkEnvironment.JvmBridge.CallConstructor(className, min, max, seed))
        {
        }

        internal RangeHyperParam(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; init; }

    }

    public class IntRangeHyperParam : RangeHyperParam<int>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.automl.IntRangeHyperParam";

        public IntRangeHyperParam(int min, int max, long seed = 0)
            : base(s_className, min, max, seed)
        {
        }

        internal IntRangeHyperParam(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        public override int GetNext() =>
            (int)Reference.Invoke("getNext");

        public override ParamPair<int> GetParamPair(Param param) =>
            new ParamPair<int>((JvmObjectReference)Reference.Invoke("getParamPair", param));

        private static IntRangeHyperParam WrapAsIntRangeHyperParam(object obj) =>
            new IntRangeHyperParam((JvmObjectReference)obj);

    }

    public class LongRangeHyperParam : RangeHyperParam<long>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.automl.LongRangeHyperParam";

        public LongRangeHyperParam(long min, long max, long seed = 0)
            : base(s_className, min, max, seed)
        {
        }

        internal LongRangeHyperParam(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        public override long GetNext() =>
            (long)Reference.Invoke("getNext");

        public override ParamPair<long> GetParamPair(Param param) =>
            new ParamPair<long>((JvmObjectReference)Reference.Invoke("getParamPair", param));

        private static LongRangeHyperParam WrapAsLongRangeHyperParam(object obj) =>
            new LongRangeHyperParam((JvmObjectReference)obj);

    }

    public class FloatRangeHyperParam : RangeHyperParam<float>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.automl.FloatRangeHyperParam";

        public FloatRangeHyperParam(float min, float max, long seed = 0)
            : base(s_className, min, max, seed)
        {
        }

        internal FloatRangeHyperParam(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        public override float GetNext() =>
            (float)Reference.Invoke("getNext");

        public override ParamPair<float> GetParamPair(Param param) =>
            new ParamPair<float>((JvmObjectReference)Reference.Invoke("getParamPair", param));

        private static FloatRangeHyperParam WrapAsFloatRangeHyperParam(object obj) =>
            new FloatRangeHyperParam((JvmObjectReference)obj);

    }

    public class DoubleRangeHyperParam : RangeHyperParam<double>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.automl.DoubleRangeHyperParam";

        public DoubleRangeHyperParam(double min, double max, long seed = 0)
            : base(s_className, min, max, seed)
        {
        }

        internal DoubleRangeHyperParam(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        public override double GetNext() =>
            (double)Reference.Invoke("getNext");

        public override ParamPair<double> GetParamPair(Param param) =>
            new ParamPair<double>((JvmObjectReference)Reference.Invoke("getParamPair", param));

        private static DoubleRangeHyperParam WrapAsDoubleRangeHyperParam(object obj) =>
            new DoubleRangeHyperParam((JvmObjectReference)obj);

    }

    public class DiscreteHyperParam<T> : Dist<T>, IJvmObjectReferenceProvider
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.automl.DiscreteHyperParam";

        /// <summary>
        /// Creates a new instance of a <see cref="DiscreteHyperParam"/>
        /// </summary>
        public DiscreteHyperParam(T[] values, long seed = 0)
            : this(SparkEnvironment.JvmBridge.CallConstructor(s_className, values, seed))
        {
        }

        internal DiscreteHyperParam(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; init; }

        public override T GetNext() =>
            (T)Reference.Invoke("getNext");
        
        public override ParamPair<T> GetParamPair(Param param) =>
            new ParamPair<T>((JvmObjectReference)Reference.Invoke("getParamPair", param));

        private static DiscreteHyperParam<T> WrapAsDiscreteHyperParam(object obj) =>
            new DiscreteHyperParam<T>((JvmObjectReference)obj);

    }


}
