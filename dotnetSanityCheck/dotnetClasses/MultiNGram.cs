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


namespace Synapse.ML.Featurize.Text
{
    /// <summary>
    /// <see cref="MultiNGram"/> implements MultiNGram
    /// </summary>
    public class MultiNGram : JavaTransformer, IJavaMLWritable, IJavaMLReadable<MultiNGram>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.featurize.text.MultiNGram";

        /// <summary>
        /// Creates a <see cref="MultiNGram"/> without any parameters.
        /// </summary>
        public MultiNGram() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="MultiNGram"/> with a UID that is used to give the
        /// <see cref="MultiNGram"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public MultiNGram(string uid) : base(s_className, uid)
        {
        }

        internal MultiNGram(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets inputCol value for <see cref="inputCol"/>
        /// </summary>
        /// <param name="inputCol">
        /// The name of the input column
        /// </param>
        /// <returns> New MultiNGram object </returns>
        public MultiNGram SetInputCol(string value) =>
            WrapAsMultiNGram(Reference.Invoke("setInputCol", (object)value));
        
        /// <summary>
        /// Sets lengths value for <see cref="lengths"/>
        /// </summary>
        /// <param name="lengths">
        /// the collection of lengths to use for ngram extraction
        /// </param>
        /// <returns> New MultiNGram object </returns>
        public MultiNGram SetLengths(int[] value) =>
            WrapAsMultiNGram(Reference.Invoke("setLengths", (object)value));
        
        /// <summary>
        /// Sets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <param name="outputCol">
        /// The name of the output column
        /// </param>
        /// <returns> New MultiNGram object </returns>
        public MultiNGram SetOutputCol(string value) =>
            WrapAsMultiNGram(Reference.Invoke("setOutputCol", (object)value));

        
        /// <summary>
        /// Gets inputCol value for <see cref="inputCol"/>
        /// </summary>
        /// <returns>
        /// inputCol: The name of the input column
        /// </returns>
        public string GetInputCol() =>
            (string)Reference.Invoke("getInputCol");
        
        
        /// <summary>
        /// Gets lengths value for <see cref="lengths"/>
        /// </summary>
        /// <returns>
        /// lengths: the collection of lengths to use for ngram extraction
        /// </returns>
        public int[] GetLengths()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getLengths");
            return (int[])jvmObject.Invoke("array");
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
        /// Loads the <see cref="MultiNGram"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="MultiNGram"/> was saved to</param>
        /// <returns>New <see cref="MultiNGram"/> object, loaded from path.</returns>
        public static MultiNGram Load(string path) => WrapAsMultiNGram(
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
        public JavaMLReader<MultiNGram> Read() =>
            new JavaMLReader<MultiNGram>((JvmObjectReference)Reference.Invoke("read"));

        private static MultiNGram WrapAsMultiNGram(object obj) =>
            new MultiNGram((JvmObjectReference)obj);

        
    }
}

        