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
    /// <see cref="CustomInputParser"/> implements CustomInputParser
    /// </summary>
    public class CustomInputParser : JavaTransformer, IJavaMLWritable, IJavaMLReadable<CustomInputParser>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.io.http.CustomInputParser";

        /// <summary>
        /// Creates a <see cref="CustomInputParser"/> without any parameters.
        /// </summary>
        public CustomInputParser() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="CustomInputParser"/> with a UID that is used to give the
        /// <see cref="CustomInputParser"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public CustomInputParser(string uid) : base(s_className, uid)
        {
        }

        internal CustomInputParser(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets inputCol value for <see cref="inputCol"/>
        /// </summary>
        /// <param name="inputCol">
        /// The name of the input column
        /// </param>
        /// <returns> New CustomInputParser object </returns>
        public CustomInputParser SetInputCol(string value) =>
            WrapAsCustomInputParser(Reference.Invoke("setInputCol", (object)value));
        
        /// <summary>
        /// Sets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <param name="outputCol">
        /// The name of the output column
        /// </param>
        /// <returns> New CustomInputParser object </returns>
        public CustomInputParser SetOutputCol(string value) =>
            WrapAsCustomInputParser(Reference.Invoke("setOutputCol", (object)value));
        
        /// <summary>
        /// Sets udfPython value for <see cref="udfPython"/>
        /// </summary>
        /// <param name="udfPython">
        /// User Defined Python Function to be applied to the DF input col
        /// </param>
        /// <returns> New CustomInputParser object </returns>
        public CustomInputParser SetUdfPython(object value) =>
            WrapAsCustomInputParser(Reference.Invoke("setUdfPython", (object)value));
        
        
        /// <summary>
        /// Sets udfScala value for <see cref="udfScala"/>
        /// </summary>
        /// <param name="udfScala">
        /// User Defined Function to be applied to the DF input col
        /// </param>
        /// <returns> New CustomInputParser object </returns>
        public CustomInputParser SetUdfScala(object value) =>
            WrapAsCustomInputParser(Reference.Invoke("setUDF", (object)value));
        
        
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
        /// Loads the <see cref="CustomInputParser"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="CustomInputParser"/> was saved to</param>
        /// <returns>New <see cref="CustomInputParser"/> object, loaded from path.</returns>
        public static CustomInputParser Load(string path) => WrapAsCustomInputParser(
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
        /// <returns>an <see cref="JavaMLReader&lt;CustomInputParser&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<CustomInputParser> Read() =>
            new JavaMLReader<CustomInputParser>((JvmObjectReference)Reference.Invoke("read"));
        private static CustomInputParser WrapAsCustomInputParser(object obj) =>
            new CustomInputParser((JvmObjectReference)obj);
        
    }
}

        