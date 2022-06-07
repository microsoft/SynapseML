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
    /// <see cref="SimpleHTTPTransformer"/> implements SimpleHTTPTransformer
    /// </summary>
    public class SimpleHTTPTransformer : JavaTransformer, IJavaMLWritable, IJavaMLReadable<SimpleHTTPTransformer>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.io.http.SimpleHTTPTransformer";

        /// <summary>
        /// Creates a <see cref="SimpleHTTPTransformer"/> without any parameters.
        /// </summary>
        public SimpleHTTPTransformer() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="SimpleHTTPTransformer"/> with a UID that is used to give the
        /// <see cref="SimpleHTTPTransformer"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public SimpleHTTPTransformer(string uid) : base(s_className, uid)
        {
        }

        internal SimpleHTTPTransformer(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets concurrency value for <see cref="concurrency"/>
        /// </summary>
        /// <param name="concurrency">
        /// max number of concurrent calls
        /// </param>
        /// <returns> New SimpleHTTPTransformer object </returns>
        public SimpleHTTPTransformer SetConcurrency(int value) =>
            WrapAsSimpleHTTPTransformer(Reference.Invoke("setConcurrency", (object)value));
        
        /// <summary>
        /// Sets concurrentTimeout value for <see cref="concurrentTimeout"/>
        /// </summary>
        /// <param name="concurrentTimeout">
        /// max number seconds to wait on futures if concurrency >= 1
        /// </param>
        /// <returns> New SimpleHTTPTransformer object </returns>
        public SimpleHTTPTransformer SetConcurrentTimeout(double value) =>
            WrapAsSimpleHTTPTransformer(Reference.Invoke("setConcurrentTimeout", (object)value));
        
        /// <summary>
        /// Sets errorCol value for <see cref="errorCol"/>
        /// </summary>
        /// <param name="errorCol">
        /// column to hold http errors
        /// </param>
        /// <returns> New SimpleHTTPTransformer object </returns>
        public SimpleHTTPTransformer SetErrorCol(string value) =>
            WrapAsSimpleHTTPTransformer(Reference.Invoke("setErrorCol", (object)value));
        
        /// <summary>
        /// Sets flattenOutputBatches value for <see cref="flattenOutputBatches"/>
        /// </summary>
        /// <param name="flattenOutputBatches">
        /// whether to flatten the output batches
        /// </param>
        /// <returns> New SimpleHTTPTransformer object </returns>
        public SimpleHTTPTransformer SetFlattenOutputBatches(bool value) =>
            WrapAsSimpleHTTPTransformer(Reference.Invoke("setFlattenOutputBatches", (object)value));
        
        /// <summary>
        /// Sets handler value for <see cref="handler"/>
        /// </summary>
        /// <param name="handler">
        /// Which strategy to use when handling requests
        /// </param>
        /// <returns> New SimpleHTTPTransformer object </returns>
        public SimpleHTTPTransformer SetHandler(object value) =>
            WrapAsSimpleHTTPTransformer(Reference.Invoke("setHandler", value));
        
        /// <summary>
        /// Sets inputCol value for <see cref="inputCol"/>
        /// </summary>
        /// <param name="inputCol">
        /// The name of the input column
        /// </param>
        /// <returns> New SimpleHTTPTransformer object </returns>
        public SimpleHTTPTransformer SetInputCol(string value) =>
            WrapAsSimpleHTTPTransformer(Reference.Invoke("setInputCol", (object)value));
        
        /// <summary>
        /// Sets inputParser value for <see cref="inputParser"/>
        /// </summary>
        /// <param name="inputParser">
        /// format to parse the column to
        /// </param>
        /// <returns> New SimpleHTTPTransformer object </returns>
        public SimpleHTTPTransformer SetInputParser(JavaTransformer value) =>
            WrapAsSimpleHTTPTransformer(Reference.Invoke("setInputParser", (object)value));
        
        /// <summary>
        /// Sets miniBatcher value for <see cref="miniBatcher"/>
        /// </summary>
        /// <param name="miniBatcher">
        /// Minibatcher to use
        /// </param>
        /// <returns> New SimpleHTTPTransformer object </returns>
        public SimpleHTTPTransformer SetMiniBatcher(JavaTransformer value) =>
            WrapAsSimpleHTTPTransformer(Reference.Invoke("setMiniBatcher", (object)value));
        
        /// <summary>
        /// Sets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <param name="outputCol">
        /// The name of the output column
        /// </param>
        /// <returns> New SimpleHTTPTransformer object </returns>
        public SimpleHTTPTransformer SetOutputCol(string value) =>
            WrapAsSimpleHTTPTransformer(Reference.Invoke("setOutputCol", (object)value));
        
        /// <summary>
        /// Sets outputParser value for <see cref="outputParser"/>
        /// </summary>
        /// <param name="outputParser">
        /// format to parse the column to
        /// </param>
        /// <returns> New SimpleHTTPTransformer object </returns>
        public SimpleHTTPTransformer SetOutputParser(JavaTransformer value) =>
            WrapAsSimpleHTTPTransformer(Reference.Invoke("setOutputParser", (object)value));
        
        /// <summary>
        /// Sets timeout value for <see cref="timeout"/>
        /// </summary>
        /// <param name="timeout">
        /// number of seconds to wait before closing the connection
        /// </param>
        /// <returns> New SimpleHTTPTransformer object </returns>
        public SimpleHTTPTransformer SetTimeout(double value) =>
            WrapAsSimpleHTTPTransformer(Reference.Invoke("setTimeout", (object)value));

        
        /// <summary>
        /// Gets concurrency value for <see cref="concurrency"/>
        /// </summary>
        /// <returns>
        /// concurrency: max number of concurrent calls
        /// </returns>
        public int GetConcurrency() =>
            (int)Reference.Invoke("getConcurrency");
        
        
        /// <summary>
        /// Gets concurrentTimeout value for <see cref="concurrentTimeout"/>
        /// </summary>
        /// <returns>
        /// concurrentTimeout: max number seconds to wait on futures if concurrency >= 1
        /// </returns>
        public double GetConcurrentTimeout() =>
            (double)Reference.Invoke("getConcurrentTimeout");
        
        
        /// <summary>
        /// Gets errorCol value for <see cref="errorCol"/>
        /// </summary>
        /// <returns>
        /// errorCol: column to hold http errors
        /// </returns>
        public string GetErrorCol() =>
            (string)Reference.Invoke("getErrorCol");
        
        
        /// <summary>
        /// Gets flattenOutputBatches value for <see cref="flattenOutputBatches"/>
        /// </summary>
        /// <returns>
        /// flattenOutputBatches: whether to flatten the output batches
        /// </returns>
        public bool GetFlattenOutputBatches() =>
            (bool)Reference.Invoke("getFlattenOutputBatches");
        
        
        /// <summary>
        /// Gets handler value for <see cref="handler"/>
        /// </summary>
        /// <returns>
        /// handler: Which strategy to use when handling requests
        /// </returns>
        public object GetHandler() => Reference.Invoke("getHandler");
        
        
        /// <summary>
        /// Gets inputCol value for <see cref="inputCol"/>
        /// </summary>
        /// <returns>
        /// inputCol: The name of the input column
        /// </returns>
        public string GetInputCol() =>
            (string)Reference.Invoke("getInputCol");
        
        
        /// <summary>
        /// Gets inputParser value for <see cref="inputParser"/>
        /// </summary>
        /// <returns>
        /// inputParser: format to parse the column to
        /// </returns>
        public JavaTransformer GetInputParser()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getInputParser");
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
        /// Gets miniBatcher value for <see cref="miniBatcher"/>
        /// </summary>
        /// <returns>
        /// miniBatcher: Minibatcher to use
        /// </returns>
        public JavaTransformer GetMiniBatcher()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getMiniBatcher");
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
        /// Gets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <returns>
        /// outputCol: The name of the output column
        /// </returns>
        public string GetOutputCol() =>
            (string)Reference.Invoke("getOutputCol");
        
        
        /// <summary>
        /// Gets outputParser value for <see cref="outputParser"/>
        /// </summary>
        /// <returns>
        /// outputParser: format to parse the column to
        /// </returns>
        public JavaTransformer GetOutputParser()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getOutputParser");
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
        /// Gets timeout value for <see cref="timeout"/>
        /// </summary>
        /// <returns>
        /// timeout: number of seconds to wait before closing the connection
        /// </returns>
        public double GetTimeout() =>
            (double)Reference.Invoke("getTimeout");

        
        /// <summary>
        /// Loads the <see cref="SimpleHTTPTransformer"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="SimpleHTTPTransformer"/> was saved to</param>
        /// <returns>New <see cref="SimpleHTTPTransformer"/> object, loaded from path.</returns>
        public static SimpleHTTPTransformer Load(string path) => WrapAsSimpleHTTPTransformer(
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
        public JavaMLReader<SimpleHTTPTransformer> Read() =>
            new JavaMLReader<SimpleHTTPTransformer>((JvmObjectReference)Reference.Invoke("read"));

        private static SimpleHTTPTransformer WrapAsSimpleHTTPTransformer(object obj) =>
            new SimpleHTTPTransformer((JvmObjectReference)obj);

        
    }
}

        