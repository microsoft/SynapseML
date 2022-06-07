// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Spark.ML.Feature.Param;
using Microsoft.Spark;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.Utils;

using SynapseML.Dotnet.Utils;
using Microsoft.Spark.ML;

namespace Synapse.ML.Stages
{
    /// <summary>
    /// <see cref="MultiColumnAdapter"/> implements MultiColumnAdapter
    /// </summary>
    public class MultiColumnAdapter : JavaEstimator<PipelineModel>, IJavaMLWritable, IJavaMLReadable<MultiColumnAdapter>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.stages.MultiColumnAdapter";

        /// <summary>
        /// Creates a <see cref="MultiColumnAdapter"/> without any parameters.
        /// </summary>
        public MultiColumnAdapter() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="MultiColumnAdapter"/> with a UID that is used to give the
        /// <see cref="MultiColumnAdapter"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public MultiColumnAdapter(string uid) : base(s_className, uid)
        {
        }

        internal MultiColumnAdapter(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets baseStage value for <see cref="baseStage"/>
        /// </summary>
        /// <param name="baseStage">
        /// base pipeline stage to apply to every column
        /// </param>
        /// <returns> New MultiColumnAdapter object </returns>
        public MultiColumnAdapter SetBaseStage(JavaPipelineStage value) =>
            WrapAsMultiColumnAdapter(Reference.Invoke("setBaseStage", (object)value));
        
        /// <summary>
        /// Sets inputCols value for <see cref="inputCols"/>
        /// </summary>
        /// <param name="inputCols">
        /// list of column names encoded as a string
        /// </param>
        /// <returns> New MultiColumnAdapter object </returns>
        public MultiColumnAdapter SetInputCols(string[] value) =>
            WrapAsMultiColumnAdapter(Reference.Invoke("setInputCols", (object)value));
        
        /// <summary>
        /// Sets outputCols value for <see cref="outputCols"/>
        /// </summary>
        /// <param name="outputCols">
        /// list of column names encoded as a string
        /// </param>
        /// <returns> New MultiColumnAdapter object </returns>
        public MultiColumnAdapter SetOutputCols(string[] value) =>
            WrapAsMultiColumnAdapter(Reference.Invoke("setOutputCols", (object)value));

        
        /// <summary>
        /// Gets baseStage value for <see cref="baseStage"/>
        /// </summary>
        /// <returns>
        /// baseStage: base pipeline stage to apply to every column
        /// </returns>
        public JavaPipelineStage GetBaseStage()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getBaseStage");
            Dictionary<string, Type> classMapping = JvmObjectUtils.ConstructJavaClassMapping(
                typeof(JavaPipelineStage),
                "s_className");
            JvmObjectUtils.TryConstructInstanceFromJvmObject(
                jvmObject,
                classMapping,
                out JavaPipelineStage instance);
            return instance;
        }
        
        
        /// <summary>
        /// Gets inputCols value for <see cref="inputCols"/>
        /// </summary>
        /// <returns>
        /// inputCols: list of column names encoded as a string
        /// </returns>
        public string[] GetInputCols() =>
            (string[])Reference.Invoke("getInputCols");
        
        
        /// <summary>
        /// Gets outputCols value for <see cref="outputCols"/>
        /// </summary>
        /// <returns>
        /// outputCols: list of column names encoded as a string
        /// </returns>
        public string[] GetOutputCols() =>
            (string[])Reference.Invoke("getOutputCols");

        /// <summary>Fits a model to the input data.</summary>
        /// <param name="dataset">The <see cref="DataFrame"/> to fit the model to.</param>
        /// <returns><see cref="PipelineModel"/></returns>
        override public PipelineModel Fit(DataFrame dataset) =>
            new PipelineModel(
                (JvmObjectReference)Reference.Invoke("fit", dataset));

        /// <summary>
        /// Loads the <see cref="MultiColumnAdapter"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="MultiColumnAdapter"/> was saved to</param>
        /// <returns>New <see cref="MultiColumnAdapter"/> object, loaded from path.</returns>
        public static MultiColumnAdapter Load(string path) => WrapAsMultiColumnAdapter(
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
        public JavaMLReader<MultiColumnAdapter> Read() =>
            new JavaMLReader<MultiColumnAdapter>((JvmObjectReference)Reference.Invoke("read"));

        private static MultiColumnAdapter WrapAsMultiColumnAdapter(object obj) =>
            new MultiColumnAdapter((JvmObjectReference)obj);

        
    }
}

        