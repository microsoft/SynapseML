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
    /// <see cref="DictionaryExamples"/> implements DictionaryExamples
    /// </summary>
    public class DictionaryExamples : JavaTransformer, IJavaMLWritable, IJavaMLReadable<DictionaryExamples>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.cognitive.DictionaryExamples";

        /// <summary>
        /// Creates a <see cref="DictionaryExamples"/> without any parameters.
        /// </summary>
        public DictionaryExamples() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="DictionaryExamples"/> with a UID that is used to give the
        /// <see cref="DictionaryExamples"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public DictionaryExamples(string uid) : base(s_className, uid)
        {
        }

        internal DictionaryExamples(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets concurrency value for <see cref="concurrency"/>
        /// </summary>
        /// <param name="concurrency">
        /// max number of concurrent calls
        /// </param>
        /// <returns> New DictionaryExamples object </returns>
        public DictionaryExamples SetConcurrency(int value) =>
            WrapAsDictionaryExamples(Reference.Invoke("setConcurrency", (object)value));
        
        /// <summary>
        /// Sets concurrentTimeout value for <see cref="concurrentTimeout"/>
        /// </summary>
        /// <param name="concurrentTimeout">
        /// max number seconds to wait on futures if concurrency >= 1
        /// </param>
        /// <returns> New DictionaryExamples object </returns>
        public DictionaryExamples SetConcurrentTimeout(double value) =>
            WrapAsDictionaryExamples(Reference.Invoke("setConcurrentTimeout", (object)value));
        
        /// <summary>
        /// Sets errorCol value for <see cref="errorCol"/>
        /// </summary>
        /// <param name="errorCol">
        /// column to hold http errors
        /// </param>
        /// <returns> New DictionaryExamples object </returns>
        public DictionaryExamples SetErrorCol(string value) =>
            WrapAsDictionaryExamples(Reference.Invoke("setErrorCol", (object)value));
        
        /// <summary>
        /// Sets fromLanguage value for <see cref="fromLanguage"/>
        /// </summary>
        /// <param name="fromLanguage">
        /// Specifies the language of the input text. The source language must be one of the supported languages included in the dictionary scope.
        /// </param>
        /// <returns> New DictionaryExamples object </returns>
        public DictionaryExamples SetFromLanguage(string value) =>
            WrapAsDictionaryExamples(Reference.Invoke("setFromLanguage", (object)value));
        
        public DictionaryExamples SetFromLanguageCol(string value) =>
            WrapAsDictionaryExamples(Reference.Invoke("setFromLanguageCol", value));
        
        /// <summary>
        /// Sets handler value for <see cref="handler"/>
        /// </summary>
        /// <param name="handler">
        /// Which strategy to use when handling requests
        /// </param>
        /// <returns> New DictionaryExamples object </returns>
        public DictionaryExamples SetHandler(object value) =>
            WrapAsDictionaryExamples(Reference.Invoke("setHandler", value));
        
        /// <summary>
        /// Sets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <param name="outputCol">
        /// The name of the output column
        /// </param>
        /// <returns> New DictionaryExamples object </returns>
        public DictionaryExamples SetOutputCol(string value) =>
            WrapAsDictionaryExamples(Reference.Invoke("setOutputCol", (object)value));
        
        /// <summary>
        /// Sets subscriptionKey value for <see cref="subscriptionKey"/>
        /// </summary>
        /// <param name="subscriptionKey">
        /// the API key to use
        /// </param>
        /// <returns> New DictionaryExamples object </returns>
        public DictionaryExamples SetSubscriptionKey(string value) =>
            WrapAsDictionaryExamples(Reference.Invoke("setSubscriptionKey", (object)value));
        
        public DictionaryExamples SetSubscriptionKeyCol(string value) =>
            WrapAsDictionaryExamples(Reference.Invoke("setSubscriptionKeyCol", value));
        
        /// <summary>
        /// Sets subscriptionRegion value for <see cref="subscriptionRegion"/>
        /// </summary>
        /// <param name="subscriptionRegion">
        /// the API region to use
        /// </param>
        /// <returns> New DictionaryExamples object </returns>
        public DictionaryExamples SetSubscriptionRegion(string value) =>
            WrapAsDictionaryExamples(Reference.Invoke("setSubscriptionRegion", (object)value));
        
        public DictionaryExamples SetSubscriptionRegionCol(string value) =>
            WrapAsDictionaryExamples(Reference.Invoke("setSubscriptionRegionCol", value));
        
        /// <summary>
        /// Sets textAndTranslation value for <see cref="textAndTranslation"/>
        /// </summary>
        /// <param name="textAndTranslation">
        ///  A string specifying the translated text previously returned by the Dictionary lookup operation.
        /// </param>
        /// <returns> New DictionaryExamples object </returns>
        public DictionaryExamples SetTextAndTranslation(TextAndTranslation[] value) =>
            WrapAsDictionaryExamples(Reference.Invoke("setTextAndTranslation", (object)value));
        
        public DictionaryExamples SetTextAndTranslationCol(string value) =>
            WrapAsDictionaryExamples(Reference.Invoke("setTextAndTranslationCol", value));
        
        /// <summary>
        /// Sets timeout value for <see cref="timeout"/>
        /// </summary>
        /// <param name="timeout">
        /// number of seconds to wait before closing the connection
        /// </param>
        /// <returns> New DictionaryExamples object </returns>
        public DictionaryExamples SetTimeout(double value) =>
            WrapAsDictionaryExamples(Reference.Invoke("setTimeout", (object)value));
        
        /// <summary>
        /// Sets toLanguage value for <see cref="toLanguage"/>
        /// </summary>
        /// <param name="toLanguage">
        /// Specifies the language of the output text. The target language must be one of the supported languages included in the dictionary scope.
        /// </param>
        /// <returns> New DictionaryExamples object </returns>
        public DictionaryExamples SetToLanguage(string value) =>
            WrapAsDictionaryExamples(Reference.Invoke("setToLanguage", (object)value));
        
        public DictionaryExamples SetToLanguageCol(string value) =>
            WrapAsDictionaryExamples(Reference.Invoke("setToLanguageCol", value));
        
        /// <summary>
        /// Sets url value for <see cref="url"/>
        /// </summary>
        /// <param name="url">
        /// Url of the service
        /// </param>
        /// <returns> New DictionaryExamples object </returns>
        public DictionaryExamples SetUrl(string value) =>
            WrapAsDictionaryExamples(Reference.Invoke("setUrl", (object)value));

        
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
        /// Gets fromLanguage value for <see cref="fromLanguage"/>
        /// </summary>
        /// <returns>
        /// fromLanguage: Specifies the language of the input text. The source language must be one of the supported languages included in the dictionary scope.
        /// </returns>
        public string GetFromLanguage() =>
            (string)Reference.Invoke("getFromLanguage");
        
        
        /// <summary>
        /// Gets handler value for <see cref="handler"/>
        /// </summary>
        /// <returns>
        /// handler: Which strategy to use when handling requests
        /// </returns>
        public object GetHandler() => Reference.Invoke("getHandler");
        
        
        /// <summary>
        /// Gets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <returns>
        /// outputCol: The name of the output column
        /// </returns>
        public string GetOutputCol() =>
            (string)Reference.Invoke("getOutputCol");
        
        
        /// <summary>
        /// Gets subscriptionKey value for <see cref="subscriptionKey"/>
        /// </summary>
        /// <returns>
        /// subscriptionKey: the API key to use
        /// </returns>
        public string GetSubscriptionKey() =>
            (string)Reference.Invoke("getSubscriptionKey");
        
        
        /// <summary>
        /// Gets subscriptionRegion value for <see cref="subscriptionRegion"/>
        /// </summary>
        /// <returns>
        /// subscriptionRegion: the API region to use
        /// </returns>
        public string GetSubscriptionRegion() =>
            (string)Reference.Invoke("getSubscriptionRegion");
        
        
        /// <summary>
        /// Gets textAndTranslation value for <see cref="textAndTranslation"/>
        /// </summary>
        /// <returns>
        /// textAndTranslation:  A string specifying the translated text previously returned by the Dictionary lookup operation.
        /// </returns>
        public TextAndTranslation[] GetTextAndTranslation()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getTextAndTranslation");
            JvmObjectReference[] jvmObjects = (JvmObjectReference[])jvmObject.Invoke("array");
            TextAndTranslation[] result =
                new TextAndTranslation[jvmObjects.Length];
            for (int i = 0; i < result.Length; i++)
            {
                result[i] = new TextAndTranslation(jvmObjects[i]);
            }
            return result;
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
        /// Gets toLanguage value for <see cref="toLanguage"/>
        /// </summary>
        /// <returns>
        /// toLanguage: Specifies the language of the output text. The target language must be one of the supported languages included in the dictionary scope.
        /// </returns>
        public string GetToLanguage() =>
            (string)Reference.Invoke("getToLanguage");
        
        
        /// <summary>
        /// Gets url value for <see cref="url"/>
        /// </summary>
        /// <returns>
        /// url: Url of the service
        /// </returns>
        public string GetUrl() =>
            (string)Reference.Invoke("getUrl");

        
        /// <summary>
        /// Loads the <see cref="DictionaryExamples"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="DictionaryExamples"/> was saved to</param>
        /// <returns>New <see cref="DictionaryExamples"/> object, loaded from path.</returns>
        public static DictionaryExamples Load(string path) => WrapAsDictionaryExamples(
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
        public JavaMLReader<DictionaryExamples> Read() =>
            new JavaMLReader<DictionaryExamples>((JvmObjectReference)Reference.Invoke("read"));

        private static DictionaryExamples WrapAsDictionaryExamples(object obj) =>
            new DictionaryExamples((JvmObjectReference)obj);

        
        public DictionaryExamples SetLocation(string value) =>
            WrapAsDictionaryExamples(Reference.Invoke("setLocation", value));
        
        public DictionaryExamples SetLinkedService(string value) =>
            WrapAsDictionaryExamples(Reference.Invoke("setLinkedService", value));

    }
}

        