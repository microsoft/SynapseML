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
using SynapseML.Dotnet.Utils;
using Synapse.ML.LightGBM.Param;


namespace Synapse.ML.Cognitive
{
    /// <summary>
    /// <see cref="AnalyzeBusinessCards"/> implements AnalyzeBusinessCards
    /// </summary>
    public class AnalyzeBusinessCards : JavaTransformer, IJavaMLWritable, IJavaMLReadable<AnalyzeBusinessCards>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.cognitive.AnalyzeBusinessCards";

        /// <summary>
        /// Creates a <see cref="AnalyzeBusinessCards"/> without any parameters.
        /// </summary>
        public AnalyzeBusinessCards() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="AnalyzeBusinessCards"/> with a UID that is used to give the
        /// <see cref="AnalyzeBusinessCards"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public AnalyzeBusinessCards(string uid) : base(s_className, uid)
        {
        }

        internal AnalyzeBusinessCards(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets backoffs value for <see cref="backoffs"/>
        /// </summary>
        /// <param name="backoffs">
        /// array of backoffs to use in the handler
        /// </param>
        /// <returns> New AnalyzeBusinessCards object </returns>
        public AnalyzeBusinessCards SetBackoffs(int[] value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setBackoffs", (object)value));
        
        /// <summary>
        /// Sets concurrency value for <see cref="concurrency"/>
        /// </summary>
        /// <param name="concurrency">
        /// max number of concurrent calls
        /// </param>
        /// <returns> New AnalyzeBusinessCards object </returns>
        public AnalyzeBusinessCards SetConcurrency(int value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setConcurrency", (object)value));
        
        /// <summary>
        /// Sets concurrentTimeout value for <see cref="concurrentTimeout"/>
        /// </summary>
        /// <param name="concurrentTimeout">
        /// max number seconds to wait on futures if concurrency >= 1
        /// </param>
        /// <returns> New AnalyzeBusinessCards object </returns>
        public AnalyzeBusinessCards SetConcurrentTimeout(double value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setConcurrentTimeout", (object)value));
        
        /// <summary>
        /// Sets errorCol value for <see cref="errorCol"/>
        /// </summary>
        /// <param name="errorCol">
        /// column to hold http errors
        /// </param>
        /// <returns> New AnalyzeBusinessCards object </returns>
        public AnalyzeBusinessCards SetErrorCol(string value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setErrorCol", (object)value));
        
        /// <summary>
        /// Sets imageBytes value for <see cref="imageBytes"/>
        /// </summary>
        /// <param name="imageBytes">
        /// bytestream of the image to use
        /// </param>
        /// <returns> New AnalyzeBusinessCards object </returns>
        public AnalyzeBusinessCards SetImageBytes(byte[] value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setImageBytes", (object)value));
        
        public AnalyzeBusinessCards SetImageBytesCol(string value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setImageBytesCol", value));
        
        /// <summary>
        /// Sets imageUrl value for <see cref="imageUrl"/>
        /// </summary>
        /// <param name="imageUrl">
        /// the url of the image to use
        /// </param>
        /// <returns> New AnalyzeBusinessCards object </returns>
        public AnalyzeBusinessCards SetImageUrl(string value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setImageUrl", (object)value));
        
        public AnalyzeBusinessCards SetImageUrlCol(string value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setImageUrlCol", value));
        
        /// <summary>
        /// Sets includeTextDetails value for <see cref="includeTextDetails"/>
        /// </summary>
        /// <param name="includeTextDetails">
        /// Include text lines and element references in the result.
        /// </param>
        /// <returns> New AnalyzeBusinessCards object </returns>
        public AnalyzeBusinessCards SetIncludeTextDetails(bool value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setIncludeTextDetails", (object)value));
        
        public AnalyzeBusinessCards SetIncludeTextDetailsCol(string value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setIncludeTextDetailsCol", value));
        
        /// <summary>
        /// Sets initialPollingDelay value for <see cref="initialPollingDelay"/>
        /// </summary>
        /// <param name="initialPollingDelay">
        /// number of milliseconds to wait before first poll for result
        /// </param>
        /// <returns> New AnalyzeBusinessCards object </returns>
        public AnalyzeBusinessCards SetInitialPollingDelay(int value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setInitialPollingDelay", (object)value));
        
        /// <summary>
        /// Sets locale value for <see cref="locale"/>
        /// </summary>
        /// <param name="locale">
        /// Locale of the receipt. Supported locales: en-AU, en-CA, en-GB, en-IN, en-US.
        /// </param>
        /// <returns> New AnalyzeBusinessCards object </returns>
        public AnalyzeBusinessCards SetLocale(string value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setLocale", (object)value));
        
        public AnalyzeBusinessCards SetLocaleCol(string value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setLocaleCol", value));
        
        /// <summary>
        /// Sets maxPollingRetries value for <see cref="maxPollingRetries"/>
        /// </summary>
        /// <param name="maxPollingRetries">
        /// number of times to poll
        /// </param>
        /// <returns> New AnalyzeBusinessCards object </returns>
        public AnalyzeBusinessCards SetMaxPollingRetries(int value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setMaxPollingRetries", (object)value));
        
        /// <summary>
        /// Sets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <param name="outputCol">
        /// The name of the output column
        /// </param>
        /// <returns> New AnalyzeBusinessCards object </returns>
        public AnalyzeBusinessCards SetOutputCol(string value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setOutputCol", (object)value));
        
        /// <summary>
        /// Sets pages value for <see cref="pages"/>
        /// </summary>
        /// <param name="pages">
        /// The page selection only leveraged for multi-page PDF and TIFF documents. Accepted input include single pages (e.g.'1, 2' -> pages 1 and 2 will be processed), finite (e.g. '2-5' -> pages 2 to 5 will be processed) and open-ended ranges (e.g. '5-' -> all the pages from page 5 will be processed & e.g. '-10' -> pages 1 to 10 will be processed). All of these can be mixed together and ranges are allowed to overlap (eg. '-5, 1, 3, 5-10' - pages 1 to 10 will be processed). The service will accept the request if it can process at least one page of the document (e.g. using '5-100' on a 5 page document is a valid input where page 5 will be processed). If no page range is provided, the entire document will be processed.
        /// </param>
        /// <returns> New AnalyzeBusinessCards object </returns>
        public AnalyzeBusinessCards SetPages(string value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setPages", (object)value));
        
        public AnalyzeBusinessCards SetPagesCol(string value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setPagesCol", value));
        
        /// <summary>
        /// Sets pollingDelay value for <see cref="pollingDelay"/>
        /// </summary>
        /// <param name="pollingDelay">
        /// number of milliseconds to wait between polling
        /// </param>
        /// <returns> New AnalyzeBusinessCards object </returns>
        public AnalyzeBusinessCards SetPollingDelay(int value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setPollingDelay", (object)value));
        
        /// <summary>
        /// Sets subscriptionKey value for <see cref="subscriptionKey"/>
        /// </summary>
        /// <param name="subscriptionKey">
        /// the API key to use
        /// </param>
        /// <returns> New AnalyzeBusinessCards object </returns>
        public AnalyzeBusinessCards SetSubscriptionKey(string value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setSubscriptionKey", (object)value));
        
        public AnalyzeBusinessCards SetSubscriptionKeyCol(string value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setSubscriptionKeyCol", value));
        
        /// <summary>
        /// Sets suppressMaxRetriesExceededException value for <see cref="suppressMaxRetriesExceededException"/>
        /// </summary>
        /// <param name="suppressMaxRetriesExceededException">
        /// set true to suppress the maxumimum retries exception and report in the error column
        /// </param>
        /// <returns> New AnalyzeBusinessCards object </returns>
        public AnalyzeBusinessCards SetSuppressMaxRetriesExceededException(bool value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setSuppressMaxRetriesExceededException", (object)value));
        
        /// <summary>
        /// Sets timeout value for <see cref="timeout"/>
        /// </summary>
        /// <param name="timeout">
        /// number of seconds to wait before closing the connection
        /// </param>
        /// <returns> New AnalyzeBusinessCards object </returns>
        public AnalyzeBusinessCards SetTimeout(double value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setTimeout", (object)value));
        
        /// <summary>
        /// Sets url value for <see cref="url"/>
        /// </summary>
        /// <param name="url">
        /// Url of the service
        /// </param>
        /// <returns> New AnalyzeBusinessCards object </returns>
        public AnalyzeBusinessCards SetUrl(string value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setUrl", (object)value));

        
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
        /// Gets errorCol value for <see cref="errorCol"/>
        /// </summary>
        /// <returns>
        /// errorCol: column to hold http errors
        /// </returns>
        public string GetErrorCol() =>
            (string)Reference.Invoke("getErrorCol");
        
        
        /// <summary>
        /// Gets imageBytes value for <see cref="imageBytes"/>
        /// </summary>
        /// <returns>
        /// imageBytes: bytestream of the image to use
        /// </returns>
        public byte[] GetImageBytes() =>
            (byte[])Reference.Invoke("getImageBytes");
        
        
        /// <summary>
        /// Gets imageUrl value for <see cref="imageUrl"/>
        /// </summary>
        /// <returns>
        /// imageUrl: the url of the image to use
        /// </returns>
        public string GetImageUrl() =>
            (string)Reference.Invoke("getImageUrl");
        
        
        /// <summary>
        /// Gets includeTextDetails value for <see cref="includeTextDetails"/>
        /// </summary>
        /// <returns>
        /// includeTextDetails: Include text lines and element references in the result.
        /// </returns>
        public bool GetIncludeTextDetails() =>
            (bool)Reference.Invoke("getIncludeTextDetails");
        
        
        /// <summary>
        /// Gets initialPollingDelay value for <see cref="initialPollingDelay"/>
        /// </summary>
        /// <returns>
        /// initialPollingDelay: number of milliseconds to wait before first poll for result
        /// </returns>
        public int GetInitialPollingDelay() =>
            (int)Reference.Invoke("getInitialPollingDelay");
        
        
        /// <summary>
        /// Gets locale value for <see cref="locale"/>
        /// </summary>
        /// <returns>
        /// locale: Locale of the receipt. Supported locales: en-AU, en-CA, en-GB, en-IN, en-US.
        /// </returns>
        public string GetLocale() =>
            (string)Reference.Invoke("getLocale");
        
        
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
        /// Gets pages value for <see cref="pages"/>
        /// </summary>
        /// <returns>
        /// pages: The page selection only leveraged for multi-page PDF and TIFF documents. Accepted input include single pages (e.g.'1, 2' -> pages 1 and 2 will be processed), finite (e.g. '2-5' -> pages 2 to 5 will be processed) and open-ended ranges (e.g. '5-' -> all the pages from page 5 will be processed & e.g. '-10' -> pages 1 to 10 will be processed). All of these can be mixed together and ranges are allowed to overlap (eg. '-5, 1, 3, 5-10' - pages 1 to 10 will be processed). The service will accept the request if it can process at least one page of the document (e.g. using '5-100' on a 5 page document is a valid input where page 5 will be processed). If no page range is provided, the entire document will be processed.
        /// </returns>
        public string GetPages() =>
            (string)Reference.Invoke("getPages");
        
        
        /// <summary>
        /// Gets pollingDelay value for <see cref="pollingDelay"/>
        /// </summary>
        /// <returns>
        /// pollingDelay: number of milliseconds to wait between polling
        /// </returns>
        public int GetPollingDelay() =>
            (int)Reference.Invoke("getPollingDelay");
        
        
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
        /// Loads the <see cref="AnalyzeBusinessCards"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="AnalyzeBusinessCards"/> was saved to</param>
        /// <returns>New <see cref="AnalyzeBusinessCards"/> object, loaded from path.</returns>
        public static AnalyzeBusinessCards Load(string path) => WrapAsAnalyzeBusinessCards(
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
        /// <returns>an <see cref="JavaMLReader&lt;AnalyzeBusinessCards&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<AnalyzeBusinessCards> Read() =>
            new JavaMLReader<AnalyzeBusinessCards>((JvmObjectReference)Reference.Invoke("read"));

        private static AnalyzeBusinessCards WrapAsAnalyzeBusinessCards(object obj) =>
            new AnalyzeBusinessCards((JvmObjectReference)obj);

        
        public AnalyzeBusinessCards SetLocation(string value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setLocation", value));
        
        public AnalyzeBusinessCards SetLinkedService(string value) =>
            WrapAsAnalyzeBusinessCards(Reference.Invoke("setLinkedService", value));

    }
}

        