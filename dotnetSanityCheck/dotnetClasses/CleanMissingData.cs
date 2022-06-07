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
using Synapse.ML.Featurize;

namespace Synapse.ML.Featurize
{
    /// <summary>
    /// <see cref="CleanMissingData"/> implements CleanMissingData
    /// </summary>
    public class CleanMissingData : JavaEstimator<CleanMissingDataModel>, IJavaMLWritable, IJavaMLReadable<CleanMissingData>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.featurize.CleanMissingData";

        /// <summary>
        /// Creates a <see cref="CleanMissingData"/> without any parameters.
        /// </summary>
        public CleanMissingData() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="CleanMissingData"/> with a UID that is used to give the
        /// <see cref="CleanMissingData"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public CleanMissingData(string uid) : base(s_className, uid)
        {
        }

        internal CleanMissingData(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets cleaningMode value for <see cref="cleaningMode"/>
        /// </summary>
        /// <param name="cleaningMode">
        /// Cleaning mode
        /// </param>
        /// <returns> New CleanMissingData object </returns>
        public CleanMissingData SetCleaningMode(string value) =>
            WrapAsCleanMissingData(Reference.Invoke("setCleaningMode", (object)value));
        
        /// <summary>
        /// Sets customValue value for <see cref="customValue"/>
        /// </summary>
        /// <param name="customValue">
        /// Custom value for replacement
        /// </param>
        /// <returns> New CleanMissingData object </returns>
        public CleanMissingData SetCustomValue(string value) =>
            WrapAsCleanMissingData(Reference.Invoke("setCustomValue", (object)value));
        
        /// <summary>
        /// Sets inputCols value for <see cref="inputCols"/>
        /// </summary>
        /// <param name="inputCols">
        /// The names of the input columns
        /// </param>
        /// <returns> New CleanMissingData object </returns>
        public CleanMissingData SetInputCols(string[] value) =>
            WrapAsCleanMissingData(Reference.Invoke("setInputCols", (object)value));
        
        /// <summary>
        /// Sets outputCols value for <see cref="outputCols"/>
        /// </summary>
        /// <param name="outputCols">
        /// The names of the output columns
        /// </param>
        /// <returns> New CleanMissingData object </returns>
        public CleanMissingData SetOutputCols(string[] value) =>
            WrapAsCleanMissingData(Reference.Invoke("setOutputCols", (object)value));

        
        /// <summary>
        /// Gets cleaningMode value for <see cref="cleaningMode"/>
        /// </summary>
        /// <returns>
        /// cleaningMode: Cleaning mode
        /// </returns>
        public string GetCleaningMode() =>
            (string)Reference.Invoke("getCleaningMode");
        
        
        /// <summary>
        /// Gets customValue value for <see cref="customValue"/>
        /// </summary>
        /// <returns>
        /// customValue: Custom value for replacement
        /// </returns>
        public string GetCustomValue() =>
            (string)Reference.Invoke("getCustomValue");
        
        
        /// <summary>
        /// Gets inputCols value for <see cref="inputCols"/>
        /// </summary>
        /// <returns>
        /// inputCols: The names of the input columns
        /// </returns>
        public string[] GetInputCols() =>
            (string[])Reference.Invoke("getInputCols");
        
        
        /// <summary>
        /// Gets outputCols value for <see cref="outputCols"/>
        /// </summary>
        /// <returns>
        /// outputCols: The names of the output columns
        /// </returns>
        public string[] GetOutputCols() =>
            (string[])Reference.Invoke("getOutputCols");

        /// <summary>Fits a model to the input data.</summary>
        /// <param name="dataset">The <see cref="DataFrame"/> to fit the model to.</param>
        /// <returns><see cref="CleanMissingDataModel"/></returns>
        override public CleanMissingDataModel Fit(DataFrame dataset) =>
            new CleanMissingDataModel(
                (JvmObjectReference)Reference.Invoke("fit", dataset));

        /// <summary>
        /// Loads the <see cref="CleanMissingData"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="CleanMissingData"/> was saved to</param>
        /// <returns>New <see cref="CleanMissingData"/> object, loaded from path.</returns>
        public static CleanMissingData Load(string path) => WrapAsCleanMissingData(
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
        public JavaMLReader<CleanMissingData> Read() =>
            new JavaMLReader<CleanMissingData>((JvmObjectReference)Reference.Invoke("read"));

        private static CleanMissingData WrapAsCleanMissingData(object obj) =>
            new CleanMissingData((JvmObjectReference)obj);

        
    }
}

        