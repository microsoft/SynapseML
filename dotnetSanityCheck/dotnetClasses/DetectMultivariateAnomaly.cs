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
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

using SynapseML.Dotnet.Utils;
using Synapse.ML.LightGBM.Param;


namespace Synapse.ML.Cognitive
{
    /// <summary>
    /// <see cref="DetectMultivariateAnomaly"/> implements DetectMultivariateAnomaly
    /// </summary>
    public class DetectMultivariateAnomaly : JavaModel<DetectMultivariateAnomaly>, IJavaMLWritable, IJavaMLReadable<DetectMultivariateAnomaly>
    {
        private static readonly string s_className = "com.microsoft.azure.synapse.ml.cognitive.DetectMultivariateAnomaly";

        /// <summary>
        /// Creates a <see cref="DetectMultivariateAnomaly"/> without any parameters.
        /// </summary>
        public DetectMultivariateAnomaly() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="DetectMultivariateAnomaly"/> with a UID that is used to give the
        /// <see cref="DetectMultivariateAnomaly"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public DetectMultivariateAnomaly(string uid) : base(s_className, uid)
        {
        }

        internal DetectMultivariateAnomaly(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets backoffs value for <see cref="backoffs"/>
        /// </summary>
        /// <param name="backoffs">
        /// array of backoffs to use in the handler
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetBackoffs(int[] value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setBackoffs", (object)value));
        
        /// <summary>
        /// Sets connectionString value for <see cref="connectionString"/>
        /// </summary>
        /// <param name="connectionString">
        /// Connection String for your storage account used for uploading files.
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetConnectionString(string value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setConnectionString", (object)value));
        
        /// <summary>
        /// Sets containerName value for <see cref="containerName"/>
        /// </summary>
        /// <param name="containerName">
        /// Container that will be used to upload files to.
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetContainerName(string value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setContainerName", (object)value));
        
        /// <summary>
        /// Sets endTime value for <see cref="endTime"/>
        /// </summary>
        /// <param name="endTime">
        /// A required field, end time of data to be used for detection/generating multivariate anomaly detection model, should be date-time.
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetEndTime(string value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setEndTime", (object)value));
        
        /// <summary>
        /// Sets endpoint value for <see cref="endpoint"/>
        /// </summary>
        /// <param name="endpoint">
        /// End Point for your storage account used for uploading files.
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetEndpoint(string value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setEndpoint", (object)value));
        
        /// <summary>
        /// Sets errorCol value for <see cref="errorCol"/>
        /// </summary>
        /// <param name="errorCol">
        /// column to hold http errors
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetErrorCol(string value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setErrorCol", (object)value));
        
        /// <summary>
        /// Sets initialPollingDelay value for <see cref="initialPollingDelay"/>
        /// </summary>
        /// <param name="initialPollingDelay">
        /// number of milliseconds to wait before first poll for result
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetInitialPollingDelay(int value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setInitialPollingDelay", (object)value));
        
        /// <summary>
        /// Sets inputCols value for <see cref="inputCols"/>
        /// </summary>
        /// <param name="inputCols">
        /// The names of the input columns
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetInputCols(string[] value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setInputCols", (object)value));
        
        /// <summary>
        /// Sets intermediateSaveDir value for <see cref="intermediateSaveDir"/>
        /// </summary>
        /// <param name="intermediateSaveDir">
        /// Directory name of which you want to save the intermediate data produced while training.
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetIntermediateSaveDir(string value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setIntermediateSaveDir", (object)value));
        
        /// <summary>
        /// Sets maxPollingRetries value for <see cref="maxPollingRetries"/>
        /// </summary>
        /// <param name="maxPollingRetries">
        /// number of times to poll
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetMaxPollingRetries(int value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setMaxPollingRetries", (object)value));
        
        /// <summary>
        /// Sets modelId value for <see cref="modelId"/>
        /// </summary>
        /// <param name="modelId">
        /// Format - uuid. Model identifier.
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetModelId(string value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setModelId", (object)value));
        
        /// <summary>
        /// Sets outputCol value for <see cref="outputCol"/>
        /// </summary>
        /// <param name="outputCol">
        /// The name of the output column
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetOutputCol(string value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setOutputCol", (object)value));
        
        /// <summary>
        /// Sets pollingDelay value for <see cref="pollingDelay"/>
        /// </summary>
        /// <param name="pollingDelay">
        /// number of milliseconds to wait between polling
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetPollingDelay(int value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setPollingDelay", (object)value));
        
        /// <summary>
        /// Sets sasToken value for <see cref="sasToken"/>
        /// </summary>
        /// <param name="sasToken">
        /// SAS Token for your storage account used for uploading files.
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetSasToken(string value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setSasToken", (object)value));
        
        /// <summary>
        /// Sets startTime value for <see cref="startTime"/>
        /// </summary>
        /// <param name="startTime">
        /// A required field, start time of data to be used for detection/generating multivariate anomaly detection model, should be date-time.
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetStartTime(string value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setStartTime", (object)value));
        
        /// <summary>
        /// Sets storageKey value for <see cref="storageKey"/>
        /// </summary>
        /// <param name="storageKey">
        /// Storage Key for your storage account used for uploading files.
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetStorageKey(string value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setStorageKey", (object)value));
        
        /// <summary>
        /// Sets storageName value for <see cref="storageName"/>
        /// </summary>
        /// <param name="storageName">
        /// Storage Name for your storage account used for uploading files.
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetStorageName(string value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setStorageName", (object)value));
        
        /// <summary>
        /// Sets subscriptionKey value for <see cref="subscriptionKey"/>
        /// </summary>
        /// <param name="subscriptionKey">
        /// the API key to use
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetSubscriptionKey(string value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setSubscriptionKey", (object)value));
        
        public DetectMultivariateAnomaly SetSubscriptionKeyCol(string value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setSubscriptionKeyCol", value));
        
        /// <summary>
        /// Sets suppressMaxRetriesExceededException value for <see cref="suppressMaxRetriesExceededException"/>
        /// </summary>
        /// <param name="suppressMaxRetriesExceededException">
        /// set true to suppress the maxumimum retries exception and report in the error column
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetSuppressMaxRetriesExceededException(bool value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setSuppressMaxRetriesExceededException", (object)value));
        
        /// <summary>
        /// Sets timestampCol value for <see cref="timestampCol"/>
        /// </summary>
        /// <param name="timestampCol">
        /// Timestamp column name
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetTimestampCol(string value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setTimestampCol", (object)value));
        
        /// <summary>
        /// Sets url value for <see cref="url"/>
        /// </summary>
        /// <param name="url">
        /// Url of the service
        /// </param>
        /// <returns> New DetectMultivariateAnomaly object </returns>
        public DetectMultivariateAnomaly SetUrl(string value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setUrl", (object)value));

        
        /// <summary>
        /// Gets backoffs value for <see cref="backoffs"/>
        /// </summary>
        /// <returns>
        /// backoffs: array of backoffs to use in the handler
        /// </returns>
        public int[] GetBackoffs() =>
            (int[])Reference.Invoke("getBackoffs");
        
        
        /// <summary>
        /// Gets connectionString value for <see cref="connectionString"/>
        /// </summary>
        /// <returns>
        /// connectionString: Connection String for your storage account used for uploading files.
        /// </returns>
        public string GetConnectionString() =>
            (string)Reference.Invoke("getConnectionString");
        
        
        /// <summary>
        /// Gets containerName value for <see cref="containerName"/>
        /// </summary>
        /// <returns>
        /// containerName: Container that will be used to upload files to.
        /// </returns>
        public string GetContainerName() =>
            (string)Reference.Invoke("getContainerName");
        
        
        /// <summary>
        /// Gets endTime value for <see cref="endTime"/>
        /// </summary>
        /// <returns>
        /// endTime: A required field, end time of data to be used for detection/generating multivariate anomaly detection model, should be date-time.
        /// </returns>
        public string GetEndTime() =>
            (string)Reference.Invoke("getEndTime");
        
        
        /// <summary>
        /// Gets endpoint value for <see cref="endpoint"/>
        /// </summary>
        /// <returns>
        /// endpoint: End Point for your storage account used for uploading files.
        /// </returns>
        public string GetEndpoint() =>
            (string)Reference.Invoke("getEndpoint");
        
        
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
        /// Gets inputCols value for <see cref="inputCols"/>
        /// </summary>
        /// <returns>
        /// inputCols: The names of the input columns
        /// </returns>
        public string[] GetInputCols() =>
            (string[])Reference.Invoke("getInputCols");
        
        
        /// <summary>
        /// Gets intermediateSaveDir value for <see cref="intermediateSaveDir"/>
        /// </summary>
        /// <returns>
        /// intermediateSaveDir: Directory name of which you want to save the intermediate data produced while training.
        /// </returns>
        public string GetIntermediateSaveDir() =>
            (string)Reference.Invoke("getIntermediateSaveDir");
        
        
        /// <summary>
        /// Gets maxPollingRetries value for <see cref="maxPollingRetries"/>
        /// </summary>
        /// <returns>
        /// maxPollingRetries: number of times to poll
        /// </returns>
        public int GetMaxPollingRetries() =>
            (int)Reference.Invoke("getMaxPollingRetries");
        
        
        /// <summary>
        /// Gets modelId value for <see cref="modelId"/>
        /// </summary>
        /// <returns>
        /// modelId: Format - uuid. Model identifier.
        /// </returns>
        public string GetModelId() =>
            (string)Reference.Invoke("getModelId");
        
        
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
        /// Gets sasToken value for <see cref="sasToken"/>
        /// </summary>
        /// <returns>
        /// sasToken: SAS Token for your storage account used for uploading files.
        /// </returns>
        public string GetSasToken() =>
            (string)Reference.Invoke("getSasToken");
        
        
        /// <summary>
        /// Gets startTime value for <see cref="startTime"/>
        /// </summary>
        /// <returns>
        /// startTime: A required field, start time of data to be used for detection/generating multivariate anomaly detection model, should be date-time.
        /// </returns>
        public string GetStartTime() =>
            (string)Reference.Invoke("getStartTime");
        
        
        /// <summary>
        /// Gets storageKey value for <see cref="storageKey"/>
        /// </summary>
        /// <returns>
        /// storageKey: Storage Key for your storage account used for uploading files.
        /// </returns>
        public string GetStorageKey() =>
            (string)Reference.Invoke("getStorageKey");
        
        
        /// <summary>
        /// Gets storageName value for <see cref="storageName"/>
        /// </summary>
        /// <returns>
        /// storageName: Storage Name for your storage account used for uploading files.
        /// </returns>
        public string GetStorageName() =>
            (string)Reference.Invoke("getStorageName");
        
        
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
        /// Gets timestampCol value for <see cref="timestampCol"/>
        /// </summary>
        /// <returns>
        /// timestampCol: Timestamp column name
        /// </returns>
        public string GetTimestampCol() =>
            (string)Reference.Invoke("getTimestampCol");
        
        
        /// <summary>
        /// Gets url value for <see cref="url"/>
        /// </summary>
        /// <returns>
        /// url: Url of the service
        /// </returns>
        public string GetUrl() =>
            (string)Reference.Invoke("getUrl");

        
        /// <summary>
        /// Loads the <see cref="DetectMultivariateAnomaly"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="DetectMultivariateAnomaly"/> was saved to</param>
        /// <returns>New <see cref="DetectMultivariateAnomaly"/> object, loaded from path.</returns>
        public static DetectMultivariateAnomaly Load(string path) => WrapAsDetectMultivariateAnomaly(
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
        public JavaMLReader<DetectMultivariateAnomaly> Read() =>
            new JavaMLReader<DetectMultivariateAnomaly>((JvmObjectReference)Reference.Invoke("read"));

        private static DetectMultivariateAnomaly WrapAsDetectMultivariateAnomaly(object obj) =>
            new DetectMultivariateAnomaly((JvmObjectReference)obj);

        
        public DetectMultivariateAnomaly SetLocation(string value) =>
            WrapAsDetectMultivariateAnomaly(Reference.Invoke("setLocation", value));

    }
}

        