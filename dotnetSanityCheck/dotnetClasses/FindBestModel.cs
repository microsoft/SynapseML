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
    /// <see cref="FindBestModel"/> implements FindBestModel
    /// </summary>
    public class FindBestModel : JavaEstimator<BestModel>, IJavaMLWritable, IJavaMLReadable<FindBestModel>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.automl.FindBestModel";

        /// <summary>
        /// Creates a <see cref="FindBestModel"/> without any parameters.
        /// </summary>
        public FindBestModel() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="FindBestModel"/> with a UID that is used to give the
        /// <see cref="FindBestModel"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public FindBestModel(string uid) : base(s_className, uid)
        {
        }

        internal FindBestModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets evaluationMetric value for <see cref="evaluationMetric"/>
        /// </summary>
        /// <param name="evaluationMetric">
        /// Metric to evaluate models with
        /// </param>
        /// <returns> New FindBestModel object </returns>
        public FindBestModel SetEvaluationMetric(string value) =>
            WrapAsFindBestModel(Reference.Invoke("setEvaluationMetric", (object)value));
        
        /// <summary>
        /// Sets models value for <see cref="models"/>
        /// </summary>
        /// <param name="models">
        /// List of models to be evaluated
        /// </param>
        /// <returns> New FindBestModel object </returns>
        public FindBestModel SetModels(JavaTransformer[] value)
        {
            var arrayList = new ArrayList(SparkEnvironment.JvmBridge);
            foreach (var v in value)
            {
                arrayList.Add(v);
            }
            return WrapAsFindBestModel(Reference.Invoke("setModels", (object)arrayList));
        }

        
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
        /// models: List of models to be evaluated
        /// </returns>
        public JavaTransformer[] GetModels()
        {
            JvmObjectReference[] jvmObjects = (JvmObjectReference[])Reference.Invoke("getModels");
            JavaTransformer[] result = new JavaTransformer[jvmObjects.Length];
            Dictionary<string, Type> classMapping = JvmObjectUtils.ConstructJavaClassMapping(
                typeof(JavaTransformer),
                "s_className");
            for (int i=0; i < jvmObjects.Length; i++)
            {
                if (JvmObjectUtils.TryConstructInstanceFromJvmObject(
                    jvmObjects[i],
                    classMapping,
                    out JavaTransformer instance))
                {
                    result[i] = instance;
                }
            }
            return result;
        }

        /// <summary>Fits a model to the input data.</summary>
        /// <param name="dataset">The <see cref="DataFrame"/> to fit the model to.</param>
        /// <returns><see cref="BestModel"/></returns>
        override public BestModel Fit(DataFrame dataset) =>
            new BestModel(
                (JvmObjectReference)Reference.Invoke("fit", dataset));

        /// <summary>
        /// Loads the <see cref="FindBestModel"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="FindBestModel"/> was saved to</param>
        /// <returns>New <see cref="FindBestModel"/> object, loaded from path.</returns>
        public static FindBestModel Load(string path) => WrapAsFindBestModel(
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
        public JavaMLReader<FindBestModel> Read() =>
            new JavaMLReader<FindBestModel>((JvmObjectReference)Reference.Invoke("read"));

        private static FindBestModel WrapAsFindBestModel(object obj) =>
            new FindBestModel((JvmObjectReference)obj);

        
    }
}

        