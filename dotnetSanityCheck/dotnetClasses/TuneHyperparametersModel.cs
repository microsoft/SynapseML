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
using Microsoft.Spark.Utils;

using SynapseML.Dotnet.Utils;


namespace Synapse.ML.Automl
{
    /// <summary>
    /// <see cref="TuneHyperparametersModel"/> implements TuneHyperparametersModel
    /// </summary>
    public class TuneHyperparametersModel : JavaModel<TuneHyperparametersModel>, IJavaMLWritable, IJavaMLReadable<TuneHyperparametersModel>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.automl.TuneHyperparametersModel";

        /// <summary>
        /// Creates a <see cref="TuneHyperparametersModel"/> without any parameters.
        /// </summary>
        public TuneHyperparametersModel() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="TuneHyperparametersModel"/> with a UID that is used to give the
        /// <see cref="TuneHyperparametersModel"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public TuneHyperparametersModel(string uid) : base(s_className, uid)
        {
        }

        internal TuneHyperparametersModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets bestMetric value for <see cref="bestMetric"/>
        /// </summary>
        /// <param name="bestMetric">
        /// the best metric from the runs
        /// </param>
        /// <returns> New TuneHyperparametersModel object </returns>
        public TuneHyperparametersModel SetBestMetric(double value) =>
            WrapAsTuneHyperparametersModel(Reference.Invoke("setBestMetric", (object)value));
        
        /// <summary>
        /// Sets bestModel value for <see cref="bestModel"/>
        /// </summary>
        /// <param name="bestModel">
        /// the best model found
        /// </param>
        /// <returns> New TuneHyperparametersModel object </returns>
        public TuneHyperparametersModel SetBestModel(JavaTransformer value) =>
            WrapAsTuneHyperparametersModel(Reference.Invoke("setBestModel", (object)value));

        
        /// <summary>
        /// Gets bestMetric value for <see cref="bestMetric"/>
        /// </summary>
        /// <returns>
        /// bestMetric: the best metric from the runs
        /// </returns>
        public double GetBestMetric() =>
            (double)Reference.Invoke("getBestMetric");
        
        
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
        /// Loads the <see cref="TuneHyperparametersModel"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="TuneHyperparametersModel"/> was saved to</param>
        /// <returns>New <see cref="TuneHyperparametersModel"/> object, loaded from path.</returns>
        public static TuneHyperparametersModel Load(string path) => WrapAsTuneHyperparametersModel(
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
        public JavaMLReader<TuneHyperparametersModel> Read() =>
            new JavaMLReader<TuneHyperparametersModel>((JvmObjectReference)Reference.Invoke("read"));

        private static TuneHyperparametersModel WrapAsTuneHyperparametersModel(object obj) =>
            new TuneHyperparametersModel((JvmObjectReference)obj);

        
    }
}

        