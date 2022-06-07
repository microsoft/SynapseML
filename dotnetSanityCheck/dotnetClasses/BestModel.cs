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
using SynapseML.Dotnet.Utils;
using Microsoft.Spark.Utils;


namespace Synapse.ML.Automl
{
    /// <summary>
    /// <see cref="BestModel"/> implements BestModel
    /// </summary>
    public class BestModel : JavaModel<BestModel>, IJavaMLWritable, IJavaMLReadable<BestModel>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.automl.BestModel";

        /// <summary>
        /// Creates a <see cref="BestModel"/> without any parameters.
        /// </summary>
        public BestModel() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="BestModel"/> with a UID that is used to give the
        /// <see cref="BestModel"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public BestModel(string uid) : base(s_className, uid)
        {
        }

        internal BestModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets allModelMetrics value for <see cref="allModelMetrics"/>
        /// </summary>
        /// <param name="allModelMetrics">
        /// all model metrics
        /// </param>
        /// <returns> New BestModel object </returns>
        public BestModel SetAllModelMetrics(DataFrame value) =>
            WrapAsBestModel(Reference.Invoke("setAllModelMetrics", (object)value));

        /// <summary>
        /// Sets bestModel value for <see cref="bestModel"/>
        /// </summary>
        /// <param name="bestModel">
        /// the best model found
        /// </param>
        /// <returns> New BestModel object </returns>
        public BestModel SetBestModel(JavaTransformer value) =>
            WrapAsBestModel(Reference.Invoke("setBestModel", (object)value));

        /// <summary>
        /// Sets bestModelMetrics value for <see cref="bestModelMetrics"/>
        /// </summary>
        /// <param name="bestModelMetrics">
        /// the metrics from the best model
        /// </param>
        /// <returns> New BestModel object </returns>
        public BestModel SetBestModelMetrics(DataFrame value) =>
            WrapAsBestModel(Reference.Invoke("setBestModelMetrics", (object)value));

        /// <summary>
        /// Sets rocCurve value for <see cref="rocCurve"/>
        /// </summary>
        /// <param name="rocCurve">
        /// the roc curve of the best model
        /// </param>
        /// <returns> New BestModel object </returns>
        public BestModel SetRocCurve(DataFrame value) =>
            WrapAsBestModel(Reference.Invoke("setRocCurve", (object)value));

        /// <summary>
        /// Sets scoredDataset value for <see cref="scoredDataset"/>
        /// </summary>
        /// <param name="scoredDataset">
        /// dataset scored by best model
        /// </param>
        /// <returns> New BestModel object </returns>
        public BestModel SetScoredDataset(DataFrame value) =>
            WrapAsBestModel(Reference.Invoke("setScoredDataset", (object)value));


        /// <summary>
        /// Gets allModelMetrics value for <see cref="allModelMetrics"/>
        /// </summary>
        /// <returns>
        /// allModelMetrics: all model metrics
        /// </returns>
        public DataFrame GetAllModelMetrics() =>
            new DataFrame((JvmObjectReference)Reference.Invoke("getAllModelMetrics"));


        /// <summary>
        /// Gets bestModel value for <see cref="bestModel"/>
        /// </summary>
        /// <returns>
        /// bestModel: the best model found
        /// </returns>
        public JavaTransformer GetBestModel()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getBestModel");
            Dictionary<string, Type> classMapping = JvmObjectUtils.ConstructJavaClassMapping(
                typeof(JavaTransformer),
                "s_className");
            JvmObjectUtils.TryConstructInstanceFromJvmObject(
                jvmObject,
                classMapping,
                out JavaTransformer instance);
            return instance;
        }


        /// <summary>
        /// Gets bestModelMetrics value for <see cref="bestModelMetrics"/>
        /// </summary>
        /// <returns>
        /// bestModelMetrics: the metrics from the best model
        /// </returns>
        public DataFrame GetBestModelMetrics() =>
            new DataFrame((JvmObjectReference)Reference.Invoke("getBestModelMetrics"));


        /// <summary>
        /// Gets rocCurve value for <see cref="rocCurve"/>
        /// </summary>
        /// <returns>
        /// rocCurve: the roc curve of the best model
        /// </returns>
        public DataFrame GetRocCurve() =>
            new DataFrame((JvmObjectReference)Reference.Invoke("getRocCurve"));


        /// <summary>
        /// Gets scoredDataset value for <see cref="scoredDataset"/>
        /// </summary>
        /// <returns>
        /// scoredDataset: dataset scored by best model
        /// </returns>
        public DataFrame GetScoredDataset() =>
            new DataFrame((JvmObjectReference)Reference.Invoke("getScoredDataset"));


        /// <summary>
        /// Loads the <see cref="BestModel"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="BestModel"/> was saved to</param>
        /// <returns>New <see cref="BestModel"/> object, loaded from path.</returns>
        public static BestModel Load(string path) => WrapAsBestModel(
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
        public JavaMLReader<BestModel> Read() =>
            new JavaMLReader<BestModel>((JvmObjectReference)Reference.Invoke("read"));

        private static BestModel WrapAsBestModel(object obj) =>
            new BestModel((JvmObjectReference)obj);


    }
}

