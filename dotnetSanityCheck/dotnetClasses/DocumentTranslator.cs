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
    /// <see cref="DocumentTranslator"/> implements DocumentTranslator
    /// </summary>
    public class DocumentTranslator : JavaTransformer, IJavaMLWritable, IJavaMLReadable<DocumentTranslator>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.cognitive.DocumentTranslator";

        /// <summary>
        /// Creates a <see cref="DocumentTranslator"/> without any parameters.
        /// </summary>
        public DocumentTranslator() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="DocumentTranslator"/> with a UID that is used to give the
        /// <see cref="DocumentTranslator"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public DocumentTranslator(string uid) : base(s_className, uid)
        {
        }

        internal DocumentTranslator(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets backoffs value for <see cref="backoffs"/>
        /// </summary>
        /// <param name="backoffs">
        /// array of backoffs to use in the handler
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetBackoffs(int[] value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setBackoffs", (object)value));
        
        /// <summary>
        /// Sets concurrency value for <see cref="concurrency"/>
        /// </summary>
        /// <param name="concurrency">
        /// max number of concurrent calls
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetConcurrency(int value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setConcurrency", (object)value));
        
        /// <summary>
        /// Sets concurrentTimeout value for <see cref="concurrentTimeout"/>
        /// </summary>
        /// <param name="concurrentTimeout">
        /// max number seconds to wait on futures if concurrency >= 1
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetConcurrentTimeout(double value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setConcurrentTimeout", (object)value));
        
        /// <summary>
        /// Sets errorCol value for <see cref="errorCol"/>
        /// </summary>
        /// <param name="errorCol">
        /// column to hold http errors
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetErrorCol(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setErrorCol", (object)value));
        
        /// <summary>
        /// Sets filterPrefix value for <see cref="filterPrefix"/>
        /// </summary>
        /// <param name="filterPrefix">
        /// A case-sensitive prefix string to filter documents in the source path for translation. For example, when using an Azure storage blob Uri, use the prefix to restrict sub folders for translation.
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetFilterPrefix(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setFilterPrefix", (object)value));
        
        public DocumentTranslator SetFilterPrefixCol(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setFilterPrefixCol", value));
        
        /// <summary>
        /// Sets filterSuffix value for <see cref="filterSuffix"/>
        /// </summary>
        /// <param name="filterSuffix">
        /// A case-sensitive suffix string to filter documents in the source path for translation. This is most often use for file extensions.
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetFilterSuffix(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setFilterSuffix", (object)value));
        
        public DocumentTranslator SetFilterSuffixCol(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setFilterSuffixCol", value));
        
        /// <summary>
        /// Sets initialPollingDelay value for <see cref="initialPollingDelay"/>
        /// </summary>
        /// <param name="initialPollingDelay">
        /// number of milliseconds to wait before first poll for result
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetInitialPollingDelay(int value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setInitialPollingDelay", (object)value));
        
        /// <summary>
        /// Sets maxPollingRetries value for <see cref="maxPollingRetries"/>
        /// </summary>
        /// <param name="maxPollingRetries">
        /// number of times to poll
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetMaxPollingRetries(int value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setMaxPollingRetries", (object)value));
        
        /// <summary>
        /// Sets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <param name="outputCol">
        /// The name of the output column
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetOutputCol(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setOutputCol", (object)value));
        
        /// <summary>
        /// Sets pollingDelay value for <see cref="pollingDelay"/>
        /// </summary>
        /// <param name="pollingDelay">
        /// number of milliseconds to wait between polling
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetPollingDelay(int value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setPollingDelay", (object)value));
        
        /// <summary>
        /// Sets serviceName value for <see cref="serviceName"/>
        /// </summary>
        /// <param name="serviceName">
        /// 
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetServiceName(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setServiceName", (object)value));
        
        /// <summary>
        /// Sets sourceLanguage value for <see cref="sourceLanguage"/>
        /// </summary>
        /// <param name="sourceLanguage">
        /// Language code. If none is specified, we will perform auto detect on the document.
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetSourceLanguage(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setSourceLanguage", (object)value));
        
        public DocumentTranslator SetSourceLanguageCol(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setSourceLanguageCol", value));
        
        /// <summary>
        /// Sets sourceStorageSource value for <see cref="sourceStorageSource"/>
        /// </summary>
        /// <param name="sourceStorageSource">
        /// Storage source of source input.
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetSourceStorageSource(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setSourceStorageSource", (object)value));
        
        public DocumentTranslator SetSourceStorageSourceCol(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setSourceStorageSourceCol", value));
        
        /// <summary>
        /// Sets sourceUrl value for <see cref="sourceUrl"/>
        /// </summary>
        /// <param name="sourceUrl">
        /// Location of the folder / container or single file with your documents.
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetSourceUrl(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setSourceUrl", (object)value));
        
        public DocumentTranslator SetSourceUrlCol(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setSourceUrlCol", value));
        
        /// <summary>
        /// Sets storageType value for <see cref="storageType"/>
        /// </summary>
        /// <param name="storageType">
        /// Storage type of the input documents source string. Required for single document translation only.
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetStorageType(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setStorageType", (object)value));
        
        public DocumentTranslator SetStorageTypeCol(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setStorageTypeCol", value));
        
        /// <summary>
        /// Sets subscriptionKey value for <see cref="subscriptionKey"/>
        /// </summary>
        /// <param name="subscriptionKey">
        /// the API key to use
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetSubscriptionKey(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setSubscriptionKey", (object)value));
        
        public DocumentTranslator SetSubscriptionKeyCol(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setSubscriptionKeyCol", value));
        
        /// <summary>
        /// Sets suppressMaxRetriesExceededException value for <see cref="suppressMaxRetriesExceededException"/>
        /// </summary>
        /// <param name="suppressMaxRetriesExceededException">
        /// set true to suppress the maxumimum retries exception and report in the error column
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetSuppressMaxRetriesExceededException(bool value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setSuppressMaxRetriesExceededException", (object)value));
        
        /// <summary>
        /// Sets targets value for <see cref="targets"/>
        /// </summary>
        /// <param name="targets">
        /// Destination for the finished translated documents.
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetTargets(TargetInput[] value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setTargets", (object)value));
        
        public DocumentTranslator SetTargetsCol(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setTargetsCol", value));
        
        /// <summary>
        /// Sets timeout value for <see cref="timeout"/>
        /// </summary>
        /// <param name="timeout">
        /// number of seconds to wait before closing the connection
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetTimeout(double value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setTimeout", (object)value));
        
        /// <summary>
        /// Sets url value for <see cref="url"/>
        /// </summary>
        /// <param name="url">
        /// Url of the service
        /// </param>
        /// <returns> New DocumentTranslator object </returns>
        public DocumentTranslator SetUrl(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setUrl", (object)value));

        
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
        /// Gets filterPrefix value for <see cref="filterPrefix"/>
        /// </summary>
        /// <returns>
        /// filterPrefix: A case-sensitive prefix string to filter documents in the source path for translation. For example, when using an Azure storage blob Uri, use the prefix to restrict sub folders for translation.
        /// </returns>
        public string GetFilterPrefix() =>
            (string)Reference.Invoke("getFilterPrefix");
        
        
        /// <summary>
        /// Gets filterSuffix value for <see cref="filterSuffix"/>
        /// </summary>
        /// <returns>
        /// filterSuffix: A case-sensitive suffix string to filter documents in the source path for translation. This is most often use for file extensions.
        /// </returns>
        public string GetFilterSuffix() =>
            (string)Reference.Invoke("getFilterSuffix");
        
        
        /// <summary>
        /// Gets initialPollingDelay value for <see cref="initialPollingDelay"/>
        /// </summary>
        /// <returns>
        /// initialPollingDelay: number of milliseconds to wait before first poll for result
        /// </returns>
        public int GetInitialPollingDelay() =>
            (int)Reference.Invoke("getInitialPollingDelay");
        
        
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
        /// Gets serviceName value for <see cref="serviceName"/>
        /// </summary>
        /// <returns>
        /// serviceName: 
        /// </returns>
        public string GetServiceName() =>
            (string)Reference.Invoke("getServiceName");
        
        
        /// <summary>
        /// Gets sourceLanguage value for <see cref="sourceLanguage"/>
        /// </summary>
        /// <returns>
        /// sourceLanguage: Language code. If none is specified, we will perform auto detect on the document.
        /// </returns>
        public string GetSourceLanguage() =>
            (string)Reference.Invoke("getSourceLanguage");
        
        
        /// <summary>
        /// Gets sourceStorageSource value for <see cref="sourceStorageSource"/>
        /// </summary>
        /// <returns>
        /// sourceStorageSource: Storage source of source input.
        /// </returns>
        public string GetSourceStorageSource() =>
            (string)Reference.Invoke("getSourceStorageSource");
        
        
        /// <summary>
        /// Gets sourceUrl value for <see cref="sourceUrl"/>
        /// </summary>
        /// <returns>
        /// sourceUrl: Location of the folder / container or single file with your documents.
        /// </returns>
        public string GetSourceUrl() =>
            (string)Reference.Invoke("getSourceUrl");
        
        
        /// <summary>
        /// Gets storageType value for <see cref="storageType"/>
        /// </summary>
        /// <returns>
        /// storageType: Storage type of the input documents source string. Required for single document translation only.
        /// </returns>
        public string GetStorageType() =>
            (string)Reference.Invoke("getStorageType");
        
        
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
        /// Gets targets value for <see cref="targets"/>
        /// </summary>
        /// <returns>
        /// targets: Destination for the finished translated documents.
        /// </returns>
        public TargetInput[] GetTargets()
        {
            JvmObjectReference jvmObject = (JvmObjectReference)Reference.Invoke("getTargets");
            JvmObjectReference[] jvmObjects = (JvmObjectReference[])jvmObject.Invoke("array");
            TargetInput[] result =
                new TargetInput[jvmObjects.Length];
            for (int i = 0; i < result.Length; i++)
            {
                result[i] = new TargetInput(jvmObjects[i]);
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
        /// Gets url value for <see cref="url"/>
        /// </summary>
        /// <returns>
        /// url: Url of the service
        /// </returns>
        public string GetUrl() =>
            (string)Reference.Invoke("getUrl");

        
        /// <summary>
        /// Loads the <see cref="DocumentTranslator"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="DocumentTranslator"/> was saved to</param>
        /// <returns>New <see cref="DocumentTranslator"/> object, loaded from path.</returns>
        public static DocumentTranslator Load(string path) => WrapAsDocumentTranslator(
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
        public JavaMLReader<DocumentTranslator> Read() =>
            new JavaMLReader<DocumentTranslator>((JvmObjectReference)Reference.Invoke("read"));

        private static DocumentTranslator WrapAsDocumentTranslator(object obj) =>
            new DocumentTranslator((JvmObjectReference)obj);

        
        public DocumentTranslator SetLinkedService(string value) =>
            WrapAsDocumentTranslator(Reference.Invoke("setLinkedService", value));

    }
}

        