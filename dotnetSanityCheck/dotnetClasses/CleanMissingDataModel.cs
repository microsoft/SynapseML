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

using SynapseML.Dotnet.Utils;


namespace Synapse.ML.Featurize
{
    /// <summary>
    /// <see cref="CleanMissingDataModel"/> implements CleanMissingDataModel
    /// </summary>
    public class CleanMissingDataModel : JavaModel<CleanMissingDataModel>, IJavaMLWritable, IJavaMLReadable<CleanMissingDataModel>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.featurize.CleanMissingDataModel";

        /// <summary>
        /// Creates a <see cref="CleanMissingDataModel"/> without any parameters.
        /// </summary>
        public CleanMissingDataModel() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="CleanMissingDataModel"/> with a UID that is used to give the
        /// <see cref="CleanMissingDataModel"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public CleanMissingDataModel(string uid) : base(s_className, uid)
        {
        }

        internal CleanMissingDataModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets colsToFill value for <see cref="colsToFill"/>
        /// </summary>
        /// <param name="colsToFill">
        /// The columns to fill with
        /// </param>
        /// <returns> New CleanMissingDataModel object </returns>
        public CleanMissingDataModel SetColsToFill(string[] value) =>
            WrapAsCleanMissingDataModel(Reference.Invoke("setColsToFill", (object)value));
        
        /// <summary>
        /// Sets fillValues value for <see cref="fillValues"/>
        /// </summary>
        /// <param name="fillValues">
        /// what to replace in the columns
        /// </param>
        /// <returns> New CleanMissingDataModel object </returns>
        public CleanMissingDataModel SetFillValues(object[] value)
        {
            var arrayList = new ArrayList(SparkEnvironment.JvmBridge);
            foreach (var v in value)
            {
                arrayList.Add(v);
            }
            return WrapAsCleanMissingDataModel(Reference.Invoke("setFillValues", (object)arrayList));
        }
        
        /// <summary>
        /// Sets inputCols value for <see cref="inputCols"/>
        /// </summary>
        /// <param name="inputCols">
        /// The names of the input columns
        /// </param>
        /// <returns> New CleanMissingDataModel object </returns>
        public CleanMissingDataModel SetInputCols(string[] value) =>
            WrapAsCleanMissingDataModel(Reference.Invoke("setInputCols", (object)value));
        
        /// <summary>
        /// Sets outputCols value for <see cref="outputCols"/>
        /// </summary>
        /// <param name="outputCols">
        /// The names of the output columns
        /// </param>
        /// <returns> New CleanMissingDataModel object </returns>
        public CleanMissingDataModel SetOutputCols(string[] value) =>
            WrapAsCleanMissingDataModel(Reference.Invoke("setOutputCols", (object)value));

        
        /// <summary>
        /// Gets colsToFill value for <see cref="colsToFill"/>
        /// </summary>
        /// <returns>
        /// colsToFill: The columns to fill with
        /// </returns>
        public string[] GetColsToFill() =>
            (string[])Reference.Invoke("getColsToFill");
        
        
        /// <summary>
        /// Gets fillValues value for <see cref="fillValues"/>
        /// </summary>
        /// <returns>
        /// fillValues: what to replace in the columns
        /// </returns>
        public object[] GetFillValues()
        {
            JvmObjectReference[] jvmObjects = (JvmObjectReference[])Reference.Invoke("getFillValues");
            object[] result = new object[jvmObjects.Length];
            for (int i = 0; i < result.Length; i++)
            {
                result[i] = SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    "org.apache.spark.api.dotnet.DotnetUtils", "mapScalaToJava", (object)jvmObjects[i]);
            }
            return result;
        }
        
        
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

        
        /// <summary>
        /// Loads the <see cref="CleanMissingDataModel"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="CleanMissingDataModel"/> was saved to</param>
        /// <returns>New <see cref="CleanMissingDataModel"/> object, loaded from path.</returns>
        public static CleanMissingDataModel Load(string path) => WrapAsCleanMissingDataModel(
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
        public JavaMLReader<CleanMissingDataModel> Read() =>
            new JavaMLReader<CleanMissingDataModel>((JvmObjectReference)Reference.Invoke("read"));

        private static CleanMissingDataModel WrapAsCleanMissingDataModel(object obj) =>
            new CleanMissingDataModel((JvmObjectReference)obj);

        
    }
}

        