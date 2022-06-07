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
using Synapse.ML.Cognitive;

namespace Synapse.ML.Cognitive
{
    /// <summary>
    /// <see cref="FormOntologyLearner"/> implements FormOntologyLearner
    /// </summary>
    public class FormOntologyLearner : JavaEstimator<FormOntologyTransformer>, IJavaMLWritable, IJavaMLReadable<FormOntologyLearner>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.cognitive.FormOntologyLearner";

        /// <summary>
        /// Creates a <see cref="FormOntologyLearner"/> without any parameters.
        /// </summary>
        public FormOntologyLearner() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="FormOntologyLearner"/> with a UID that is used to give the
        /// <see cref="FormOntologyLearner"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public FormOntologyLearner(string uid) : base(s_className, uid)
        {
        }

        internal FormOntologyLearner(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets inputCol value for <see cref="inputCol"/>
        /// </summary>
        /// <param name="inputCol">
        /// The name of the input column
        /// </param>
        /// <returns> New FormOntologyLearner object </returns>
        public FormOntologyLearner SetInputCol(string value) =>
            WrapAsFormOntologyLearner(Reference.Invoke("setInputCol", (object)value));
        
        /// <summary>
        /// Sets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <param name="outputCol">
        /// The name of the output column
        /// </param>
        /// <returns> New FormOntologyLearner object </returns>
        public FormOntologyLearner SetOutputCol(string value) =>
            WrapAsFormOntologyLearner(Reference.Invoke("setOutputCol", (object)value));
        
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
        /// <summary>Fits a model to the input data.</summary>
        /// <param name="dataset">The <see cref="DataFrame"/> to fit the model to.</param>
        /// <returns><see cref="FormOntologyTransformer"/></returns>
        override public FormOntologyTransformer Fit(DataFrame dataset) =>
            new FormOntologyTransformer(
                (JvmObjectReference)Reference.Invoke("fit", dataset));
        /// <summary>
        /// Loads the <see cref="FormOntologyLearner"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="FormOntologyLearner"/> was saved to</param>
        /// <returns>New <see cref="FormOntologyLearner"/> object, loaded from path.</returns>
        public static FormOntologyLearner Load(string path) => WrapAsFormOntologyLearner(
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
        /// <returns>an <see cref="JavaMLReader&lt;FormOntologyLearner&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<FormOntologyLearner> Read() =>
            new JavaMLReader<FormOntologyLearner>((JvmObjectReference)Reference.Invoke("read"));
        private static FormOntologyLearner WrapAsFormOntologyLearner(object obj) =>
            new FormOntologyLearner((JvmObjectReference)obj);
        
    }
}

        