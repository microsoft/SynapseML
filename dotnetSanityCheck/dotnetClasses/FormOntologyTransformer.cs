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


namespace Synapse.ML.Cognitive
{
    /// <summary>
    /// <see cref="FormOntologyTransformer"/> implements FormOntologyTransformer
    /// </summary>
    public class FormOntologyTransformer : JavaModel<FormOntologyTransformer>, IJavaMLWritable, IJavaMLReadable<FormOntologyTransformer>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.cognitive.FormOntologyTransformer";

        /// <summary>
        /// Creates a <see cref="FormOntologyTransformer"/> without any parameters.
        /// </summary>
        public FormOntologyTransformer() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="FormOntologyTransformer"/> with a UID that is used to give the
        /// <see cref="FormOntologyTransformer"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public FormOntologyTransformer(string uid) : base(s_className, uid)
        {
        }

        internal FormOntologyTransformer(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets inputCol value for <see cref="inputCol"/>
        /// </summary>
        /// <param name="inputCol">
        /// The name of the input column
        /// </param>
        /// <returns> New FormOntologyTransformer object </returns>
        public FormOntologyTransformer SetInputCol(string value) =>
            WrapAsFormOntologyTransformer(Reference.Invoke("setInputCol", (object)value));
        
        /// <summary>
        /// Sets ontology value for <see cref="ontology"/>
        /// </summary>
        /// <param name="ontology">
        /// The ontology to cast values to
        /// </param>
        /// <returns> New FormOntologyTransformer object </returns>
        public FormOntologyTransformer SetOntology(DataType value) =>
            WrapAsFormOntologyTransformer(Reference.Invoke("setOntology",
            DataType.FromJson(Reference.Jvm, value.Json)));
        
        
        /// <summary>
        /// Sets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <param name="outputCol">
        /// The name of the output column
        /// </param>
        /// <returns> New FormOntologyTransformer object </returns>
        public FormOntologyTransformer SetOutputCol(string value) =>
            WrapAsFormOntologyTransformer(Reference.Invoke("setOutputCol", (object)value));
        
        /// <summary>
        /// Gets inputCol value for <see cref="inputCol"/>
        /// </summary>
        /// <returns>
        /// inputCol: The name of the input column
        /// </returns>
        public string GetInputCol() =>
            (string)Reference.Invoke("getInputCol");
        
        /// <summary>
        /// Gets ontology value for <see cref="ontology"/>
        /// </summary>
        /// <returns>
        /// ontology: The ontology to cast values to
        /// </returns>
        public DataType GetOntology()
        {
            var jvmObject = (JvmObjectReference)Reference.Invoke("getOntology");
            var json = (string)jvmObject.Invoke("json");
            return DataType.ParseDataType(json);
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
        /// Loads the <see cref="FormOntologyTransformer"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="FormOntologyTransformer"/> was saved to</param>
        /// <returns>New <see cref="FormOntologyTransformer"/> object, loaded from path.</returns>
        public static FormOntologyTransformer Load(string path) => WrapAsFormOntologyTransformer(
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
        /// <returns>an <see cref="JavaMLReader&lt;FormOntologyTransformer&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<FormOntologyTransformer> Read() =>
            new JavaMLReader<FormOntologyTransformer>((JvmObjectReference)Reference.Invoke("read"));
        private static FormOntologyTransformer WrapAsFormOntologyTransformer(object obj) =>
            new FormOntologyTransformer((JvmObjectReference)obj);
        
    }
}

        