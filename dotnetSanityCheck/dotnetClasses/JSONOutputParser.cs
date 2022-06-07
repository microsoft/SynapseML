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


namespace Synapse.ML.Io.Http
{
    /// <summary>
    /// <see cref="JSONOutputParser"/> implements JSONOutputParser
    /// </summary>
    public class JSONOutputParser : JavaTransformer, IJavaMLWritable, IJavaMLReadable<JSONOutputParser>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.io.http.JSONOutputParser";

        /// <summary>
        /// Creates a <see cref="JSONOutputParser"/> without any parameters.
        /// </summary>
        public JSONOutputParser() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="JSONOutputParser"/> with a UID that is used to give the
        /// <see cref="JSONOutputParser"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public JSONOutputParser(string uid) : base(s_className, uid)
        {
        }

        internal JSONOutputParser(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets dataType value for <see cref="dataType"/>
        /// </summary>
        /// <param name="dataType">
        /// format to parse the column to
        /// </param>
        /// <returns> New JSONOutputParser object </returns>
        public JSONOutputParser SetDataType(DataType value) =>
            WrapAsJSONOutputParser(Reference.Invoke("setDataType",
            DataType.FromJson(Reference.Jvm, value.Json)));
        
        /// <summary>
        /// Sets inputCol value for <see cref="inputCol"/>
        /// </summary>
        /// <param name="inputCol">
        /// The name of the input column
        /// </param>
        /// <returns> New JSONOutputParser object </returns>
        public JSONOutputParser SetInputCol(string value) =>
            WrapAsJSONOutputParser(Reference.Invoke("setInputCol", (object)value));
        
        /// <summary>
        /// Sets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <param name="outputCol">
        /// The name of the output column
        /// </param>
        /// <returns> New JSONOutputParser object </returns>
        public JSONOutputParser SetOutputCol(string value) =>
            WrapAsJSONOutputParser(Reference.Invoke("setOutputCol", (object)value));
        
        /// <summary>
        /// Sets postProcessor value for <see cref="postProcessor"/>
        /// </summary>
        /// <param name="postProcessor">
        /// optional transformation to postprocess json output
        /// </param>
        /// <returns> New JSONOutputParser object </returns>
        public JSONOutputParser SetPostProcessor(JavaTransformer value) =>
            WrapAsJSONOutputParser(Reference.Invoke("setPostProcessor", (object)value));

        
        /// <summary>
        /// Gets dataType value for <see cref="dataType"/>
        /// </summary>
        /// <returns>
        /// dataType: format to parse the column to
        /// </returns>
        public DataType GetDataType()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getDataType");
            string json = (string)jvmObject.Invoke("json");
            return DataType.ParseDataType(json);
        }
        
        
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
        /// Gets postProcessor value for <see cref="postProcessor"/>
        /// </summary>
        /// <returns>
        /// postProcessor: optional transformation to postprocess json output
        /// </returns>
        public JavaTransformer GetPostProcessor()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getPostProcessor");
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
        /// Loads the <see cref="JSONOutputParser"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="JSONOutputParser"/> was saved to</param>
        /// <returns>New <see cref="JSONOutputParser"/> object, loaded from path.</returns>
        public static JSONOutputParser Load(string path) => WrapAsJSONOutputParser(
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
        public JavaMLReader<JSONOutputParser> Read() =>
            new JavaMLReader<JSONOutputParser>((JvmObjectReference)Reference.Invoke("read"));

        private static JSONOutputParser WrapAsJSONOutputParser(object obj) =>
            new JSONOutputParser((JvmObjectReference)obj);

        
    }
}

        