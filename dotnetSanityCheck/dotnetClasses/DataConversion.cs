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


namespace Synapse.ML.Featurize
{
    /// <summary>
    /// <see cref="DataConversion"/> implements DataConversion
    /// </summary>
    public class DataConversion : JavaTransformer, IJavaMLWritable, IJavaMLReadable<DataConversion>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.featurize.DataConversion";

        /// <summary>
        /// Creates a <see cref="DataConversion"/> without any parameters.
        /// </summary>
        public DataConversion() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="DataConversion"/> with a UID that is used to give the
        /// <see cref="DataConversion"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public DataConversion(string uid) : base(s_className, uid)
        {
        }

        internal DataConversion(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets cols value for <see cref="cols"/>
        /// </summary>
        /// <param name="cols">
        /// Comma separated list of columns whose type will be converted
        /// </param>
        /// <returns> New DataConversion object </returns>
        public DataConversion SetCols(string[] value) =>
            WrapAsDataConversion(Reference.Invoke("setCols", (object)value));
        
        /// <summary>
        /// Sets convertTo value for <see cref="convertTo"/>
        /// </summary>
        /// <param name="convertTo">
        /// The result type
        /// </param>
        /// <returns> New DataConversion object </returns>
        public DataConversion SetConvertTo(string value) =>
            WrapAsDataConversion(Reference.Invoke("setConvertTo", (object)value));
        
        /// <summary>
        /// Sets dateTimeFormat value for <see cref="dateTimeFormat"/>
        /// </summary>
        /// <param name="dateTimeFormat">
        /// Format for DateTime when making DateTime:String conversions
        /// </param>
        /// <returns> New DataConversion object </returns>
        public DataConversion SetDateTimeFormat(string value) =>
            WrapAsDataConversion(Reference.Invoke("setDateTimeFormat", (object)value));
        
        /// <summary>
        /// Gets cols value for <see cref="cols"/>
        /// </summary>
        /// <returns>
        /// cols: Comma separated list of columns whose type will be converted
        /// </returns>
        public string[] GetCols() =>
            (string[])Reference.Invoke("getCols");
        
        
        /// <summary>
        /// Gets convertTo value for <see cref="convertTo"/>
        /// </summary>
        /// <returns>
        /// convertTo: The result type
        /// </returns>
        public string GetConvertTo() =>
            (string)Reference.Invoke("getConvertTo");
        
        
        /// <summary>
        /// Gets dateTimeFormat value for <see cref="dateTimeFormat"/>
        /// </summary>
        /// <returns>
        /// dateTimeFormat: Format for DateTime when making DateTime:String conversions
        /// </returns>
        public string GetDateTimeFormat() =>
            (string)Reference.Invoke("getDateTimeFormat");
        
        /// <summary>
        /// Loads the <see cref="DataConversion"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="DataConversion"/> was saved to</param>
        /// <returns>New <see cref="DataConversion"/> object, loaded from path.</returns>
        public static DataConversion Load(string path) => WrapAsDataConversion(
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
        /// <returns>an <see cref="JavaMLReader&lt;DataConversion&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<DataConversion> Read() =>
            new JavaMLReader<DataConversion>((JvmObjectReference)Reference.Invoke("read"));
        private static DataConversion WrapAsDataConversion(object obj) =>
            new DataConversion((JvmObjectReference)obj);
        
    }
}

        