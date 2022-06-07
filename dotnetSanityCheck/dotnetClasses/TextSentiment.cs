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


namespace Synapse.ML.Cognitive
{
    /// <summary>
    /// <see cref="TextSentiment"/> implements TextSentiment
    /// </summary>
    public class TextSentiment : JavaTransformer, IJavaMLWritable, IJavaMLReadable<TextSentiment>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.cognitive.TextSentiment";

        /// <summary>
        /// Creates a <see cref="TextSentiment"/> without any parameters.
        /// </summary>
        public TextSentiment() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="TextSentiment"/> with a UID that is used to give the
        /// <see cref="TextSentiment"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public TextSentiment(string uid) : base(s_className, uid)
        {
        }

        internal TextSentiment(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets concurrency value for <see cref="concurrency"/>
        /// </summary>
        /// <param name="concurrency">
        /// max number of concurrent calls
        /// </param>
        /// <returns> New TextSentiment object </returns>
        public TextSentiment SetConcurrency(int value) =>
            WrapAsTextSentiment(Reference.Invoke("setConcurrency", (object)value));
        
        /// <summary>
        /// Sets concurrentTimeout value for <see cref="concurrentTimeout"/>
        /// </summary>
        /// <param name="concurrentTimeout">
        /// max number seconds to wait on futures if concurrency >= 1
        /// </param>
        /// <returns> New TextSentiment object </returns>
        public TextSentiment SetConcurrentTimeout(double value) =>
            WrapAsTextSentiment(Reference.Invoke("setConcurrentTimeout", (object)value));
        
        /// <summary>
        /// Sets errorCol value for <see cref="errorCol"/>
        /// </summary>
        /// <param name="errorCol">
        /// column to hold http errors
        /// </param>
        /// <returns> New TextSentiment object </returns>
        public TextSentiment SetErrorCol(string value) =>
            WrapAsTextSentiment(Reference.Invoke("setErrorCol", (object)value));
        
        /// <summary>
        /// Sets handler value for <see cref="handler"/>
        /// </summary>
        /// <param name="handler">
        /// Which strategy to use when handling requests
        /// </param>
        /// <returns> New TextSentiment object </returns>
        public TextSentiment SetHandler(object value) =>
            WrapAsTextSentiment(Reference.Invoke("setHandler", value));
        
        /// <summary>
        /// Sets language value for <see cref="language"/>
        /// </summary>
        /// <param name="language">
        /// the language code of the text (optional for some services)
        /// </param>
        /// <returns> New TextSentiment object </returns>
        public TextSentiment SetLanguage(string[] value) =>
            WrapAsTextSentiment(Reference.Invoke("setLanguage", (object)value));
        
        public TextSentiment SetLanguageCol(string value) =>
            WrapAsTextSentiment(Reference.Invoke("setLanguageCol", value));
        
        /// <summary>
        /// Sets modelVersion value for <see cref="modelVersion"/>
        /// </summary>
        /// <param name="modelVersion">
        /// This value indicates which model will be used for scoring. If a model-version is not specified, the API should default to the latest, non-preview version.
        /// </param>
        /// <returns> New TextSentiment object </returns>
        public TextSentiment SetModelVersion(string value) =>
            WrapAsTextSentiment(Reference.Invoke("setModelVersion", (object)value));
        
        public TextSentiment SetModelVersionCol(string value) =>
            WrapAsTextSentiment(Reference.Invoke("setModelVersionCol", value));
        
        /// <summary>
        /// Sets opinionMining value for <see cref="opinionMining"/>
        /// </summary>
        /// <param name="opinionMining">
        /// if set to true, response will contain not only sentiment prediction but also opinion mining (aspect-based sentiment analysis) results.
        /// </param>
        /// <returns> New TextSentiment object </returns>
        public TextSentiment SetOpinionMining(bool value) =>
            WrapAsTextSentiment(Reference.Invoke("setOpinionMining", (object)value));
        
        public TextSentiment SetOpinionMiningCol(string value) =>
            WrapAsTextSentiment(Reference.Invoke("setOpinionMiningCol", value));
        
        /// <summary>
        /// Sets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <param name="outputCol">
        /// The name of the output column
        /// </param>
        /// <returns> New TextSentiment object </returns>
        public TextSentiment SetOutputCol(string value) =>
            WrapAsTextSentiment(Reference.Invoke("setOutputCol", (object)value));
        
        /// <summary>
        /// Sets showStats value for <see cref="showStats"/>
        /// </summary>
        /// <param name="showStats">
        /// if set to true, response will contain input and document level statistics.
        /// </param>
        /// <returns> New TextSentiment object </returns>
        public TextSentiment SetShowStats(bool value) =>
            WrapAsTextSentiment(Reference.Invoke("setShowStats", (object)value));
        
        public TextSentiment SetShowStatsCol(string value) =>
            WrapAsTextSentiment(Reference.Invoke("setShowStatsCol", value));
        
        /// <summary>
        /// Sets stringIndexType value for <see cref="stringIndexType"/>
        /// </summary>
        /// <param name="stringIndexType">
        /// Specifies the method used to interpret string offsets. Defaults to Text Elements (Graphemes) according to Unicode v8.0.0. For additional information see https://aka.ms/text-analytics-offsets
        /// </param>
        /// <returns> New TextSentiment object </returns>
        public TextSentiment SetStringIndexType(string value) =>
            WrapAsTextSentiment(Reference.Invoke("setStringIndexType", (object)value));
        
        public TextSentiment SetStringIndexTypeCol(string value) =>
            WrapAsTextSentiment(Reference.Invoke("setStringIndexTypeCol", value));
        
        /// <summary>
        /// Sets subscriptionKey value for <see cref="subscriptionKey"/>
        /// </summary>
        /// <param name="subscriptionKey">
        /// the API key to use
        /// </param>
        /// <returns> New TextSentiment object </returns>
        public TextSentiment SetSubscriptionKey(string value) =>
            WrapAsTextSentiment(Reference.Invoke("setSubscriptionKey", (object)value));
        
        public TextSentiment SetSubscriptionKeyCol(string value) =>
            WrapAsTextSentiment(Reference.Invoke("setSubscriptionKeyCol", value));
        
        /// <summary>
        /// Sets text value for <see cref="text"/>
        /// </summary>
        /// <param name="text">
        /// the text in the request body
        /// </param>
        /// <returns> New TextSentiment object </returns>
        public TextSentiment SetText(string[] value) =>
            WrapAsTextSentiment(Reference.Invoke("setText", (object)value));
        
        public TextSentiment SetTextCol(string value) =>
            WrapAsTextSentiment(Reference.Invoke("setTextCol", value));
        
        /// <summary>
        /// Sets timeout value for <see cref="timeout"/>
        /// </summary>
        /// <param name="timeout">
        /// number of seconds to wait before closing the connection
        /// </param>
        /// <returns> New TextSentiment object </returns>
        public TextSentiment SetTimeout(double value) =>
            WrapAsTextSentiment(Reference.Invoke("setTimeout", (object)value));
        
        /// <summary>
        /// Sets url value for <see cref="url"/>
        /// </summary>
        /// <param name="url">
        /// Url of the service
        /// </param>
        /// <returns> New TextSentiment object </returns>
        public TextSentiment SetUrl(string value) =>
            WrapAsTextSentiment(Reference.Invoke("setUrl", (object)value));

        
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
        /// Gets handler value for <see cref="handler"/>
        /// </summary>
        /// <returns>
        /// handler: Which strategy to use when handling requests
        /// </returns>
        public object GetHandler() => Reference.Invoke("getHandler");
        
        
        /// <summary>
        /// Gets language value for <see cref="language"/>
        /// </summary>
        /// <returns>
        /// language: the language code of the text (optional for some services)
        /// </returns>
        public string[] GetLanguage() =>
            (string[])Reference.Invoke("getLanguage");
        
        
        /// <summary>
        /// Gets modelVersion value for <see cref="modelVersion"/>
        /// </summary>
        /// <returns>
        /// modelVersion: This value indicates which model will be used for scoring. If a model-version is not specified, the API should default to the latest, non-preview version.
        /// </returns>
        public string GetModelVersion() =>
            (string)Reference.Invoke("getModelVersion");
        
        
        /// <summary>
        /// Gets opinionMining value for <see cref="opinionMining"/>
        /// </summary>
        /// <returns>
        /// opinionMining: if set to true, response will contain not only sentiment prediction but also opinion mining (aspect-based sentiment analysis) results.
        /// </returns>
        public bool GetOpinionMining() =>
            (bool)Reference.Invoke("getOpinionMining");
        
        
        /// <summary>
        /// Gets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <returns>
        /// outputCol: The name of the output column
        /// </returns>
        public string GetOutputCol() =>
            (string)Reference.Invoke("getOutputCol");
        
        
        /// <summary>
        /// Gets showStats value for <see cref="showStats"/>
        /// </summary>
        /// <returns>
        /// showStats: if set to true, response will contain input and document level statistics.
        /// </returns>
        public bool GetShowStats() =>
            (bool)Reference.Invoke("getShowStats");
        
        
        /// <summary>
        /// Gets stringIndexType value for <see cref="stringIndexType"/>
        /// </summary>
        /// <returns>
        /// stringIndexType: Specifies the method used to interpret string offsets. Defaults to Text Elements (Graphemes) according to Unicode v8.0.0. For additional information see https://aka.ms/text-analytics-offsets
        /// </returns>
        public string GetStringIndexType() =>
            (string)Reference.Invoke("getStringIndexType");
        
        
        /// <summary>
        /// Gets subscriptionKey value for <see cref="subscriptionKey"/>
        /// </summary>
        /// <returns>
        /// subscriptionKey: the API key to use
        /// </returns>
        public string GetSubscriptionKey() =>
            (string)Reference.Invoke("getSubscriptionKey");
        
        
        /// <summary>
        /// Gets text value for <see cref="text"/>
        /// </summary>
        /// <returns>
        /// text: the text in the request body
        /// </returns>
        public string[] GetText() =>
            (string[])Reference.Invoke("getText");
        
        
        /// <summary>
        /// Gets timeout value for <see cref="timeout"/>
        /// </summary>
        /// <returns>
        /// timeout: number of seconds to wait before closing the connection
        /// </returns>
        public double GetTimeout() =>
            (double)Reference.Invoke("getTimeout");
        
        
        /// <summary>
        /// Gets url value for <see cref="url"/>
        /// </summary>
        /// <returns>
        /// url: Url of the service
        /// </returns>
        public string GetUrl() =>
            (string)Reference.Invoke("getUrl");

        
        /// <summary>
        /// Loads the <see cref="TextSentiment"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="TextSentiment"/> was saved to</param>
        /// <returns>New <see cref="TextSentiment"/> object, loaded from path.</returns>
        public static TextSentiment Load(string path) => WrapAsTextSentiment(
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
        public JavaMLReader<TextSentiment> Read() =>
            new JavaMLReader<TextSentiment>((JvmObjectReference)Reference.Invoke("read"));

        private static TextSentiment WrapAsTextSentiment(object obj) =>
            new TextSentiment((JvmObjectReference)obj);

        
        public TextSentiment SetLocation(string value) =>
            WrapAsTextSentiment(Reference.Invoke("setLocation", value));
        
        public TextSentiment SetLinkedService(string value) =>
            WrapAsTextSentiment(Reference.Invoke("setLinkedService", value));

    }
}

        