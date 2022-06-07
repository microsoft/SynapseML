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


namespace Synapse.ML.Io.Http
{
    /// <summary>
    /// <see cref="CustomOutputParser"/> implements CustomOutputParser
    /// </summary>
    public class CustomOutputParser : JavaTransformer, IJavaMLWritable, IJavaMLReadable<CustomOutputParser>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.io.http.CustomOutputParser";

        /// <summary>
        /// Creates a <see cref="CustomOutputParser"/> without any parameters.
        /// </summary>
        public CustomOutputParser() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="CustomOutputParser"/> with a UID that is used to give the
        /// <see cref="CustomOutputParser"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public CustomOutputParser(string uid) : base(s_className, uid)
        {
        }

        internal CustomOutputParser(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets inputCol value for <see cref="inputCol"/>
        /// </summary>
        /// <param name="inputCol">
        /// The name of the input column
        /// </param>
        /// <returns> New CustomOutputParser object </returns>
        public CustomOutputParser SetInputCol(string value) =>
            WrapAsCustomOutputParser(Reference.Invoke("setInputCol", (object)value));
        
        /// <summary>
        /// Sets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <param name="outputCol">
        /// The name of the output column
        /// </param>
        /// <returns> New CustomOutputParser object </returns>
        public CustomOutputParser SetOutputCol(string value) =>
            WrapAsCustomOutputParser(Reference.Invoke("setOutputCol", (object)value));
        
        /// <summary>
        /// Sets udfPython value for <see cref="udfPython"/>
        /// </summary>
        /// <param name="udfPython">
        /// User Defined Python Function to be applied to the DF input col
        /// </param>
        /// <returns> New CustomOutputParser object </returns>
        public CustomOutputParser SetUdfPython(object value) =>
            WrapAsCustomOutputParser(Reference.Invoke("setUdfPython", (object)value));
        
        
        /// <summary>
        /// Sets udfScala value for <see cref="udfScala"/>
        /// </summary>
        /// <param name="udfScala">
        /// User Defined Function to be applied to the DF input col
        /// </param>
        /// <returns> New CustomOutputParser object </returns>
        public CustomOutputParser SetUdfScala(object value) =>
            WrapAsCustomOutputParser(Reference.Invoke("setUDF", (object)value));
        
        
        /// <summary>
        /// Gets inputCol value for <see cref="inputCol"/>
        /// </summary>
        /// <returns>
        /// inputCol: The name of the input column
        /// </returns>
        public string GetInputCol() =>
            (string)Reference.Invoke("getInputCol");
        
        
        /// <summary>
        /// Gets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <returns>
        /// outputCol: The name of the output column
        /// </returns>
        public string GetOutputCol() =>
            (string)Reference.Invoke("getOutputCol");
        
        /// <summary>
        /// Gets udfPython value for <see cref="udfPython"/>
        /// </summary>
        /// <returns>
        /// udfPython: User Defined Python Function to be applied to the DF input col
        /// </returns>
        public object GetUdfPython() => Reference.Invoke("getUdfPython");
        
        /// <summary>
        /// Gets udfScala value for <see cref="udfScala"/>
        /// </summary>
        /// <returns>
        /// udfScala: User Defined Function to be applied to the DF input col
        /// </returns>
        public object GetUdfScala() => Reference.Invoke("getUdfScala");
        
        /// <summary>
        /// Loads the <see cref="CustomOutputParser"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="CustomOutputParser"/> was saved to</param>
        /// <returns>New <see cref="CustomOutputParser"/> object, loaded from path.</returns>
        public static CustomOutputParser Load(string path) => WrapAsCustomOutputParser(
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
        /// <returns>an <see cref="JavaMLReader&lt;CustomOutputParser&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<CustomOutputParser> Read() =>
            new JavaMLReader<CustomOutputParser>((JvmObjectReference)Reference.Invoke("read"));
        private static CustomOutputParser WrapAsCustomOutputParser(object obj) =>
            new CustomOutputParser((JvmObjectReference)obj);
        
    }
}

        