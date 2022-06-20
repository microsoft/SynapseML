// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.


using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.ML.Feature.Param;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Interop.Internal.Java.Util;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.Utils;
using SynapseML.Dotnet.Utils;
using Synapse.ML.LightGBM.Param;


namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// <see cref="StringIndexerModel"/> implements StringIndexerModel
    /// </summary>
    public class StringIndexerModel : JavaModel<StringIndexerModel>, IJavaMLWritable, IJavaMLReadable<StringIndexerModel>
    {
        private static readonly string s_className = "org.apache.spark.ml.feature.StringIndexerModel";

        /// <summary>
        /// Creates a <see cref="StringIndexerModel"/> with a UID that is used to give the
        /// <see cref="StringIndexerModel"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        /// <param name="labelsArray">Array of ordered list of labels, corresponding to indices to be assigned for each input column.</param>
        public StringIndexerModel(string uid, string[][] labelsArray)
            : this(SparkEnvironment.JvmBridge.CallConstructor(s_className, uid, labelsArray))
        {
        }

        /// <summary>
        /// Creates a <see cref="StringIndexerModel"/> with a UID that is used to give the
        /// <see cref="StringIndexerModel"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        /// <param name="labels">Array of ordered list of labels, corresponding to indices to be assigned for each input column.</param>
        public StringIndexerModel(string uid, string[] labels)
            : this(SparkEnvironment.JvmBridge.CallConstructor(s_className, uid, labels))
        {
        }


        /// <summary>
        /// Creates a <see cref="StringIndexerModel"/> without any parameters.
        /// </summary>
        public StringIndexerModel(string[][] labelsArray)
            : this(SparkEnvironment.JvmBridge.CallConstructor(s_className, labelsArray))
        {
        }

        /// <summary>
        /// Creates a <see cref="StringIndexerModel"/> without any parameters.
        /// </summary>
        public StringIndexerModel(string[] labels)
            : this(SparkEnvironment.JvmBridge.CallConstructor(s_className, labels))
        {
        }

        internal StringIndexerModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets handleInvalid value for <see cref="handleInvalid"/>
        /// </summary>
        /// <param name="handleInvalid">
        /// How to handle invalid data (unseen labels or NULL values). Options are 'skip' (filter out rows with invalid data), error (throw an error), or 'keep' (put invalid data in a special additional bucket, at index numLabels).
        /// </param>
        /// <returns> New StringIndexerModel object </returns>
        public StringIndexerModel SetHandleInvalid(string value) =>
            WrapAsStringIndexerModel(Reference.Invoke("setHandleInvalid", (object)value));
        
        /// <summary>
        /// Sets inputCol value for <see cref="inputCol"/>
        /// </summary>
        /// <param name="inputCol">
        /// input column name
        /// </param>
        /// <returns> New StringIndexerModel object </returns>
        public StringIndexerModel SetInputCol(string value) =>
            WrapAsStringIndexerModel(Reference.Invoke("setInputCol", (object)value));
        
        /// <summary>
        /// Sets inputCols value for <see cref="inputCols"/>
        /// </summary>
        /// <param name="inputCols">
        /// input column names
        /// </param>
        /// <returns> New StringIndexerModel object </returns>
        public StringIndexerModel SetInputCols(string[] value) =>
            WrapAsStringIndexerModel(Reference.Invoke("setInputCols", (object)value));
        
        /// <summary>
        /// Sets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <param name="outputCol">
        /// output column name
        /// </param>
        /// <returns> New StringIndexerModel object </returns>
        public StringIndexerModel SetOutputCol(string value) =>
            WrapAsStringIndexerModel(Reference.Invoke("setOutputCol", (object)value));
        
        /// <summary>
        /// Sets outputCols value for <see cref="outputCols"/>
        /// </summary>
        /// <param name="outputCols">
        /// output column names
        /// </param>
        /// <returns> New StringIndexerModel object </returns>
        public StringIndexerModel SetOutputCols(string[] value) =>
            WrapAsStringIndexerModel(Reference.Invoke("setOutputCols", (object)value));
        
        /// <summary>
        /// Sets stringOrderType value for <see cref="stringOrderType"/>
        /// </summary>
        /// <param name="stringOrderType">
        /// How to order labels of string column. The first label after ordering is assigned an index of 0. Supported options: frequencyDesc, frequencyAsc, alphabetDesc, alphabetAsc.
        /// </param>
        /// <returns> New StringIndexerModel object </returns>
        public StringIndexerModel SetStringOrderType(string value) =>
            WrapAsStringIndexerModel(Reference.Invoke("setStringOrderType", (object)value));

        
        /// <summary>
        /// Gets handleInvalid value for <see cref="handleInvalid"/>
        /// </summary>
        /// <returns>
        /// handleInvalid: How to handle invalid data (unseen labels or NULL values). Options are 'skip' (filter out rows with invalid data), error (throw an error), or 'keep' (put invalid data in a special additional bucket, at index numLabels).
        /// </returns>
        public string GetHandleInvalid() =>
            (string)Reference.Invoke("getHandleInvalid");
        
        
        /// <summary>
        /// Gets inputCol value for <see cref="inputCol"/>
        /// </summary>
        /// <returns>
        /// inputCol: input column name
        /// </returns>
        public string GetInputCol() =>
            (string)Reference.Invoke("getInputCol");
        
        
        /// <summary>
        /// Gets inputCols value for <see cref="inputCols"/>
        /// </summary>
        /// <returns>
        /// inputCols: input column names
        /// </returns>
        public string[] GetInputCols() =>
            (string[])Reference.Invoke("getInputCols");
        
        
        /// <summary>
        /// Gets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <returns>
        /// outputCol: output column name
        /// </returns>
        public string GetOutputCol() =>
            (string)Reference.Invoke("getOutputCol");
        
        
        /// <summary>
        /// Gets outputCols value for <see cref="outputCols"/>
        /// </summary>
        /// <returns>
        /// outputCols: output column names
        /// </returns>
        public string[] GetOutputCols() =>
            (string[])Reference.Invoke("getOutputCols");
        
        
        /// <summary>
        /// Gets stringOrderType value for <see cref="stringOrderType"/>
        /// </summary>
        /// <returns>
        /// stringOrderType: How to order labels of string column. The first label after ordering is assigned an index of 0. Supported options: frequencyDesc, frequencyAsc, alphabetDesc, alphabetAsc.
        /// </returns>
        public string GetStringOrderType() =>
            (string)Reference.Invoke("getStringOrderType");

        
        /// <summary>
        /// Loads the <see cref="StringIndexerModel"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="StringIndexerModel"/> was saved to</param>
        /// <returns>New <see cref="StringIndexerModel"/> object, loaded from path.</returns>
        public static StringIndexerModel Load(string path) => WrapAsStringIndexerModel(
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
        
        /// <summary>
        /// Get the corresponding JavaMLReader instance.
        /// </summary>
        /// <returns>an <see cref="JavaMLReader&lt;StringIndexerModel&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<StringIndexerModel> Read() =>
            new JavaMLReader<StringIndexerModel>((JvmObjectReference)Reference.Invoke("read"));

        private static StringIndexerModel WrapAsStringIndexerModel(object obj) =>
            new StringIndexerModel((JvmObjectReference)obj);

        
    }
}

        