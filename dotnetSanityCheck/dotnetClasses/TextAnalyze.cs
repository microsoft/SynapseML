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
    /// <see cref="TextAnalyze"/> implements TextAnalyze
    /// </summary>
    public class TextAnalyze : JavaTransformer, IJavaMLWritable, IJavaMLReadable<TextAnalyze>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.cognitive.TextAnalyze";

        /// <summary>
        /// Creates a <see cref="TextAnalyze"/> without any parameters.
        /// </summary>
        public TextAnalyze() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="TextAnalyze"/> with a UID that is used to give the
        /// <see cref="TextAnalyze"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public TextAnalyze(string uid) : base(s_className, uid)
        {
        }

        internal TextAnalyze(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets backoffs value for <see cref="backoffs"/>
        /// </summary>
        /// <param name="backoffs">
        /// array of backoffs to use in the handler
        /// </param>
        /// <returns> New TextAnalyze object </returns>
        public TextAnalyze SetBackoffs(int[] value) =>
            WrapAsTextAnalyze(Reference.Invoke("setBackoffs", (object)value));
        
        /// <summary>
        /// Sets concurrency value for <see cref="concurrency"/>
        /// </summary>
        /// <param name="concurrency">
        /// max number of concurrent calls
        /// </param>
        /// <returns> New TextAnalyze object </returns>
        public TextAnalyze SetConcurrency(int value) =>
            WrapAsTextAnalyze(Reference.Invoke("setConcurrency", (object)value));
        
        /// <summary>
        /// Sets concurrentTimeout value for <see cref="concurrentTimeout"/>
        /// </summary>
        /// <param name="concurrentTimeout">
        /// max number seconds to wait on futures if concurrency >= 1
        /// </param>
        /// <returns> New TextAnalyze object </returns>
        public TextAnalyze SetConcurrentTimeout(double value) =>
            WrapAsTextAnalyze(Reference.Invoke("setConcurrentTimeout", (object)value));
        
        /// <summary>
        /// Sets entityLinkingTasks value for <see cref="entityLinkingTasks"/>
        /// </summary>
        /// <param name="entityLinkingTasks">
        /// the entity linking tasks to perform on submitted documents
        /// </param>
        /// <returns> New TextAnalyze object </returns>
        public TextAnalyze SetEntityLinkingTasks(TextAnalyzeTask[] value) =>
            WrapAsTextAnalyze(Reference.Invoke("setEntityLinkingTasks", (object)value));
        
        /// <summary>
        /// Sets entityRecognitionPiiTasks value for <see cref="entityRecognitionPiiTasks"/>
        /// </summary>
        /// <param name="entityRecognitionPiiTasks">
        /// the entity recognition pii tasks to perform on submitted documents
        /// </param>
        /// <returns> New TextAnalyze object </returns>
        public TextAnalyze SetEntityRecognitionPiiTasks(TextAnalyzeTask[] value) =>
            WrapAsTextAnalyze(Reference.Invoke("setEntityRecognitionPiiTasks", (object)value));
        
        /// <summary>
        /// Sets entityRecognitionTasks value for <see cref="entityRecognitionTasks"/>
        /// </summary>
        /// <param name="entityRecognitionTasks">
        /// the entity recognition tasks to perform on submitted documents
        /// </param>
        /// <returns> New TextAnalyze object </returns>
        public TextAnalyze SetEntityRecognitionTasks(TextAnalyzeTask[] value) =>
            WrapAsTextAnalyze(Reference.Invoke("setEntityRecognitionTasks", (object)value));
        
        /// <summary>
        /// Sets errorCol value for <see cref="errorCol"/>
        /// </summary>
        /// <param name="errorCol">
        /// column to hold http errors
        /// </param>
        /// <returns> New TextAnalyze object </returns>
        public TextAnalyze SetErrorCol(string value) =>
            WrapAsTextAnalyze(Reference.Invoke("setErrorCol", (object)value));
        
        /// <summary>
        /// Sets initialPollingDelay value for <see cref="initialPollingDelay"/>
        /// </summary>
        /// <param name="initialPollingDelay">
        /// number of milliseconds to wait before first poll for result
        /// </param>
        /// <returns> New TextAnalyze object </returns>
        public TextAnalyze SetInitialPollingDelay(int value) =>
            WrapAsTextAnalyze(Reference.Invoke("setInitialPollingDelay", (object)value));
        
        /// <summary>
        /// Sets keyPhraseExtractionTasks value for <see cref="keyPhraseExtractionTasks"/>
        /// </summary>
        /// <param name="keyPhraseExtractionTasks">
        /// the key phrase extraction tasks to perform on submitted documents
        /// </param>
        /// <returns> New TextAnalyze object </returns>
        public TextAnalyze SetKeyPhraseExtractionTasks(TextAnalyzeTask[] value) =>
            WrapAsTextAnalyze(Reference.Invoke("setKeyPhraseExtractionTasks", (object)value));
        
        /// <summary>
        /// Sets language value for <see cref="language"/>
        /// </summary>
        /// <param name="language">
        /// the language code of the text (optional for some services)
        /// </param>
        /// <returns> New TextAnalyze object </returns>
        public TextAnalyze SetLanguage(string[] value) =>
            WrapAsTextAnalyze(Reference.Invoke("setLanguage", (object)value));
        
        public TextAnalyze SetLanguageCol(string value) =>
            WrapAsTextAnalyze(Reference.Invoke("setLanguageCol", value));
        
        /// <summary>
        /// Sets maxPollingRetries value for <see cref="maxPollingRetries"/>
        /// </summary>
        /// <param name="maxPollingRetries">
        /// number of times to poll
        /// </param>
        /// <returns> New TextAnalyze object </returns>
        public TextAnalyze SetMaxPollingRetries(int value) =>
            WrapAsTextAnalyze(Reference.Invoke("setMaxPollingRetries", (object)value));
        
        /// <summary>
        /// Sets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <param name="outputCol">
        /// The name of the output column
        /// </param>
        /// <returns> New TextAnalyze object </returns>
        public TextAnalyze SetOutputCol(string value) =>
            WrapAsTextAnalyze(Reference.Invoke("setOutputCol", (object)value));
        
        /// <summary>
        /// Sets pollingDelay value for <see cref="pollingDelay"/>
        /// </summary>
        /// <param name="pollingDelay">
        /// number of milliseconds to wait between polling
        /// </param>
        /// <returns> New TextAnalyze object </returns>
        public TextAnalyze SetPollingDelay(int value) =>
            WrapAsTextAnalyze(Reference.Invoke("setPollingDelay", (object)value));
        
        /// <summary>
        /// Sets sentimentAnalysisTasks value for <see cref="sentimentAnalysisTasks"/>
        /// </summary>
        /// <param name="sentimentAnalysisTasks">
        /// the sentiment analysis tasks to perform on submitted documents
        /// </param>
        /// <returns> New TextAnalyze object </returns>
        public TextAnalyze SetSentimentAnalysisTasks(TextAnalyzeTask[] value) =>
            WrapAsTextAnalyze(Reference.Invoke("setSentimentAnalysisTasks", (object)value));
        
        /// <summary>
        /// Sets subscriptionKey value for <see cref="subscriptionKey"/>
        /// </summary>
        /// <param name="subscriptionKey">
        /// the API key to use
        /// </param>
        /// <returns> New TextAnalyze object </returns>
        public TextAnalyze SetSubscriptionKey(string value) =>
            WrapAsTextAnalyze(Reference.Invoke("setSubscriptionKey", (object)value));
        
        public TextAnalyze SetSubscriptionKeyCol(string value) =>
            WrapAsTextAnalyze(Reference.Invoke("setSubscriptionKeyCol", value));
        
        /// <summary>
        /// Sets suppressMaxRetriesExceededException value for <see cref="suppressMaxRetriesExceededException"/>
        /// </summary>
        /// <param name="suppressMaxRetriesExceededException">
        /// set true to suppress the maxumimum retries exception and report in the error column
        /// </param>
        /// <returns> New TextAnalyze object </returns>
        public TextAnalyze SetSuppressMaxRetriesExceededException(bool value) =>
            WrapAsTextAnalyze(Reference.Invoke("setSuppressMaxRetriesExceededException", (object)value));
        
        /// <summary>
        /// Sets text value for <see cref="text"/>
        /// </summary>
        /// <param name="text">
        /// the text in the request body
        /// </param>
        /// <returns> New TextAnalyze object </returns>
        public TextAnalyze SetText(string[] value) =>
            WrapAsTextAnalyze(Reference.Invoke("setText", (object)value));
        
        public TextAnalyze SetTextCol(string value) =>
            WrapAsTextAnalyze(Reference.Invoke("setTextCol", value));
        
        /// <summary>
        /// Sets timeout value for <see cref="timeout"/>
        /// </summary>
        /// <param name="timeout">
        /// number of seconds to wait before closing the connection
        /// </param>
        /// <returns> New TextAnalyze object </returns>
        public TextAnalyze SetTimeout(double value) =>
            WrapAsTextAnalyze(Reference.Invoke("setTimeout", (object)value));
        
        /// <summary>
        /// Sets url value for <see cref="url"/>
        /// </summary>
        /// <param name="url">
        /// Url of the service
        /// </param>
        /// <returns> New TextAnalyze object </returns>
        public TextAnalyze SetUrl(string value) =>
            WrapAsTextAnalyze(Reference.Invoke("setUrl", (object)value));

        
        /// <summary>
        /// Gets backoffs value for <see cref="backoffs"/>
        /// </summary>
        /// <returns>
        /// backoffs: array of backoffs to use in the handler
        /// </returns>
        public int[] GetBackoffs() =>
            (int[])Reference.Invoke("getBackoffs");
        
        
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
        /// Gets entityLinkingTasks value for <see cref="entityLinkingTasks"/>
        /// </summary>
        /// <returns>
        /// entityLinkingTasks: the entity linking tasks to perform on submitted documents
        /// </returns>
        public TextAnalyzeTask[] GetEntityLinkingTasks()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getEntityLinkingTasks");
            JvmObjectReference[] jvmObjects = (JvmObjectReference[])jvmObject.Invoke("array");
            TextAnalyzeTask[] result =
                new TextAnalyzeTask[jvmObjects.Length];
            for (int i = 0; i < result.Length; i++)
            {
                result[i] = new TextAnalyzeTask(jvmObjects[i]);
            }
            return result;
        }
        
        
        /// <summary>
        /// Gets entityRecognitionPiiTasks value for <see cref="entityRecognitionPiiTasks"/>
        /// </summary>
        /// <returns>
        /// entityRecognitionPiiTasks: the entity recognition pii tasks to perform on submitted documents
        /// </returns>
        public TextAnalyzeTask[] GetEntityRecognitionPiiTasks() =>
            (TextAnalyzeTask[])Reference.Invoke("getEntityRecognitionPiiTasks");
        
        
        /// <summary>
        /// Gets entityRecognitionTasks value for <see cref="entityRecognitionTasks"/>
        /// </summary>
        /// <returns>
        /// entityRecognitionTasks: the entity recognition tasks to perform on submitted documents
        /// </returns>
        public TextAnalyzeTask[] GetEntityRecognitionTasks() =>
            (TextAnalyzeTask[])Reference.Invoke("getEntityRecognitionTasks");
        
        
        /// <summary>
        /// Gets errorCol value for <see cref="errorCol"/>
        /// </summary>
        /// <returns>
        /// errorCol: column to hold http errors
        /// </returns>
        public string GetErrorCol() =>
            (string)Reference.Invoke("getErrorCol");
        
        
        /// <summary>
        /// Gets initialPollingDelay value for <see cref="initialPollingDelay"/>
        /// </summary>
        /// <returns>
        /// initialPollingDelay: number of milliseconds to wait before first poll for result
        /// </returns>
        public int GetInitialPollingDelay() =>
            (int)Reference.Invoke("getInitialPollingDelay");
        
        
        /// <summary>
        /// Gets keyPhraseExtractionTasks value for <see cref="keyPhraseExtractionTasks"/>
        /// </summary>
        /// <returns>
        /// keyPhraseExtractionTasks: the key phrase extraction tasks to perform on submitted documents
        /// </returns>
        public TextAnalyzeTask[] GetKeyPhraseExtractionTasks() =>
            (TextAnalyzeTask[])Reference.Invoke("getKeyPhraseExtractionTasks");
        
        
        /// <summary>
        /// Gets language value for <see cref="language"/>
        /// </summary>
        /// <returns>
        /// language: the language code of the text (optional for some services)
        /// </returns>
        public string[] GetLanguage() =>
            (string[])Reference.Invoke("getLanguage");
        
        
        /// <summary>
        /// Gets maxPollingRetries value for <see cref="maxPollingRetries"/>
        /// </summary>
        /// <returns>
        /// maxPollingRetries: number of times to poll
        /// </returns>
        public int GetMaxPollingRetries() =>
            (int)Reference.Invoke("getMaxPollingRetries");
        
        
        /// <summary>
        /// Gets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <returns>
        /// outputCol: The name of the output column
        /// </returns>
        public string GetOutputCol() =>
            (string)Reference.Invoke("getOutputCol");
        
        
        /// <summary>
        /// Gets pollingDelay value for <see cref="pollingDelay"/>
        /// </summary>
        /// <returns>
        /// pollingDelay: number of milliseconds to wait between polling
        /// </returns>
        public int GetPollingDelay() =>
            (int)Reference.Invoke("getPollingDelay");
        
        
        /// <summary>
        /// Gets sentimentAnalysisTasks value for <see cref="sentimentAnalysisTasks"/>
        /// </summary>
        /// <returns>
        /// sentimentAnalysisTasks: the sentiment analysis tasks to perform on submitted documents
        /// </returns>
        public TextAnalyzeTask[] GetSentimentAnalysisTasks() =>
            (TextAnalyzeTask[])Reference.Invoke("getSentimentAnalysisTasks");
        
        
        /// <summary>
        /// Gets subscriptionKey value for <see cref="subscriptionKey"/>
        /// </summary>
        /// <returns>
        /// subscriptionKey: the API key to use
        /// </returns>
        public string GetSubscriptionKey() =>
            (string)Reference.Invoke("getSubscriptionKey");
        
        
        /// <summary>
        /// Gets suppressMaxRetriesExceededException value for <see cref="suppressMaxRetriesExceededException"/>
        /// </summary>
        /// <returns>
        /// suppressMaxRetriesExceededException: set true to suppress the maxumimum retries exception and report in the error column
        /// </returns>
        public bool GetSuppressMaxRetriesExceededException() =>
            (bool)Reference.Invoke("getSuppressMaxRetriesExceededException");
        
        
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
        /// Loads the <see cref="TextAnalyze"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="TextAnalyze"/> was saved to</param>
        /// <returns>New <see cref="TextAnalyze"/> object, loaded from path.</returns>
        public static TextAnalyze Load(string path) => WrapAsTextAnalyze(
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
        public JavaMLReader<TextAnalyze> Read() =>
            new JavaMLReader<TextAnalyze>((JvmObjectReference)Reference.Invoke("read"));

        private static TextAnalyze WrapAsTextAnalyze(object obj) =>
            new TextAnalyze((JvmObjectReference)obj);

        
        public TextAnalyze SetLocation(string value) =>
            WrapAsTextAnalyze(Reference.Invoke("setLocation", value));
        
        public TextAnalyze SetLinkedService(string value) =>
            WrapAsTextAnalyze(Reference.Invoke("setLinkedService", value));

    }
}

        