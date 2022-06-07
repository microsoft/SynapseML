// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Spark.ML.Feature.Param;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.Interop.Internal.Java.Util;
using Microsoft.Spark.Utils;

using SynapseML.Dotnet.Utils;
using Synapse.ML.Automl;

namespace Synapse.ML.Automl
{
    /// <summary>
    /// <see cref="TuneHyperparameters"/> implements TuneHyperparameters
    /// </summary>
    public class TuneHyperparameters : JavaEstimator<TuneHyperparametersModel>, IJavaMLWritable, IJavaMLReadable<TuneHyperparameters>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.automl.TuneHyperparameters";

        /// <summary>
        /// Creates a <see cref="TuneHyperparameters"/> without any parameters.
        /// </summary>
        public TuneHyperparameters() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="TuneHyperparameters"/> with a UID that is used to give the
        /// <see cref="TuneHyperparameters"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public TuneHyperparameters(string uid) : base(s_className, uid)
        {
        }

        internal TuneHyperparameters(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets evaluationMetric value for <see cref="evaluationMetric"/>
        /// </summary>
        /// <param name="evaluationMetric">
        /// Metric to evaluate models with
        /// </param>
        /// <returns> New TuneHyperparameters object </returns>
        public TuneHyperparameters SetEvaluationMetric(string value) =>
            WrapAsTuneHyperparameters(Reference.Invoke("setEvaluationMetric", (object)value));
        
        /// <summary>
        /// Sets models value for <see cref="models"/>
        /// </summary>
        /// <param name="models">
        /// Estimators to run
        /// </param>
        /// <returns> New TuneHyperparameters object </returns>
        public TuneHyperparameters SetModels(IEstimator<object>[] value)
        {
            var arrayList = new ArrayList(SparkEnvironment.JvmBridge);
            foreach (var v in value)
            {
                arrayList.Add(v);
            }
            return WrapAsTuneHyperparameters(Reference.Invoke("setModels", (object)arrayList));
        }
        
        /// <summary>
        /// Sets numFolds value for <see cref="numFolds"/>
        /// </summary>
        /// <param name="numFolds">
        /// Number of folds
        /// </param>
        /// <returns> New TuneHyperparameters object </returns>
        public TuneHyperparameters SetNumFolds(int value) =>
            WrapAsTuneHyperparameters(Reference.Invoke("setNumFolds", (object)value));
        
        /// <summary>
        /// Sets numRuns value for <see cref="numRuns"/>
        /// </summary>
        /// <param name="numRuns">
        /// Termination criteria for randomized search
        /// </param>
        /// <returns> New TuneHyperparameters object </returns>
        public TuneHyperparameters SetNumRuns(int value) =>
            WrapAsTuneHyperparameters(Reference.Invoke("setNumRuns", (object)value));
        
        /// <summary>
        /// Sets parallelism value for <see cref="parallelism"/>
        /// </summary>
        /// <param name="parallelism">
        /// The number of models to run in parallel
        /// </param>
        /// <returns> New TuneHyperparameters object </returns>
        public TuneHyperparameters SetParallelism(int value) =>
            WrapAsTuneHyperparameters(Reference.Invoke("setParallelism", (object)value));
        
        /// <summary>
        /// Sets paramSpace value for <see cref="paramSpace"/>
        /// </summary>
        /// <param name="paramSpace">
        /// Parameter space for generating hyperparameters
        /// </param>
        /// <returns> New TuneHyperparameters object </returns>
        public TuneHyperparameters SetParamSpace(object value) =>
            WrapAsTuneHyperparameters(Reference.Invoke("setParamSpace", value));
        
        /// <summary>
        /// Sets seed value for <see cref="seed"/>
        /// </summary>
        /// <param name="seed">
        /// Random number generator seed
        /// </param>
        /// <returns> New TuneHyperparameters object </returns>
        public TuneHyperparameters SetSeed(long value) =>
            WrapAsTuneHyperparameters(Reference.Invoke("setSeed", (object)value));

        
        /// <summary>
        /// Gets evaluationMetric value for <see cref="evaluationMetric"/>
        /// </summary>
        /// <returns>
        /// evaluationMetric: Metric to evaluate models with
        /// </returns>
        public string GetEvaluationMetric() =>
            (string)Reference.Invoke("getEvaluationMetric");
        
        
        /// <summary>
        /// Gets models value for <see cref="models"/>
        /// </summary>
        /// <returns>
        /// models: Estimators to run
        /// </returns>
        public IEstimator<object>[] GetModels()
        {
            JvmObjectReference[] jvmObjects = (JvmObjectReference[])Reference.Invoke("getModels");
            IEstimator<object>[] result = new IEstimator<object>[jvmObjects.Length];
            Dictionary<string, Type> classMapping = JvmObjectUtils.ConstructJavaClassMapping(
                typeof(JavaPipelineStage),
                "s_className");
            for (int i=0; i < jvmObjects.Length; i++)
            {
                if (JvmObjectUtils.TryConstructInstanceFromJvmObject(
                    jvmObjects[i],
                    classMapping,
                    out IEstimator<object> instance))
                {
                    result[i] = instance;
                }
            }
            return result;
        }
        
        
        /// <summary>
        /// Gets numFolds value for <see cref="numFolds"/>
        /// </summary>
        /// <returns>
        /// numFolds: Number of folds
        /// </returns>
        public int GetNumFolds() =>
            (int)Reference.Invoke("getNumFolds");
        
        
        /// <summary>
        /// Gets numRuns value for <see cref="numRuns"/>
        /// </summary>
        /// <returns>
        /// numRuns: Termination criteria for randomized search
        /// </returns>
        public int GetNumRuns() =>
            (int)Reference.Invoke("getNumRuns");
        
        
        /// <summary>
        /// Gets parallelism value for <see cref="parallelism"/>
        /// </summary>
        /// <returns>
        /// parallelism: The number of models to run in parallel
        /// </returns>
        public int GetParallelism() =>
            (int)Reference.Invoke("getParallelism");
        
        
        /// <summary>
        /// Gets paramSpace value for <see cref="paramSpace"/>
        /// </summary>
        /// <returns>
        /// paramSpace: Parameter space for generating hyperparameters
        /// </returns>
        public object GetParamSpace() => Reference.Invoke("getParamSpace");
        
        
        /// <summary>
        /// Gets seed value for <see cref="seed"/>
        /// </summary>
        /// <returns>
        /// seed: Random number generator seed
        /// </returns>
        public long GetSeed() =>
            (long)Reference.Invoke("getSeed");

        /// <summary>Fits a model to the input data.</summary>
        /// <param name="dataset">The <see cref="DataFrame"/> to fit the model to.</param>
        /// <returns><see cref="TuneHyperparametersModel"/></returns>
        override public TuneHyperparametersModel Fit(DataFrame dataset) =>
            new TuneHyperparametersModel(
                (JvmObjectReference)Reference.Invoke("fit", dataset));

        /// <summary>
        /// Loads the <see cref="TuneHyperparameters"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="TuneHyperparameters"/> was saved to</param>
        /// <returns>New <see cref="TuneHyperparameters"/> object, loaded from path.</returns>
        public static TuneHyperparameters Load(string path) => WrapAsTuneHyperparameters(
            SparkEnvironment.JvmBridge.CallStaticJavaMethod(s_className, "load", path));
        
        /// <summary>
        /// Saves the object so that it can be loaded later using Load. Note that these objects
        /// can be shared with Scala by Loading or Saving in Scala.
        /// </summary>
        /// <param name="path">The path to save the object to</param>
        public void Save(string path) => Reference.Invoke("save", path);
        
        /// <returns>a <see cref="JavaMLWriter"/> instance for this ML instance.</returns>
        public JavaMLWriter Write() =>
            new JavaMLWriter((JvmObjectReference)Reference.Invoke("write"));
        
        /// <returns>an <see cref="JavaMLReader"/> instance for this ML instance.</returns>
        public JavaMLReader<TuneHyperparameters> Read() =>
            new JavaMLReader<TuneHyperparameters>((JvmObjectReference)Reference.Invoke("read"));

        private static TuneHyperparameters WrapAsTuneHyperparameters(object obj) =>
            new TuneHyperparameters((JvmObjectReference)obj);

        
    }
}

        