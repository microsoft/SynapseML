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


namespace Microsoft.Spark.ML.Recommendation
{
    /// <summary>
    /// <see cref="ALSModel"/> implements ALSModel
    /// </summary>
    public class ALSModel : JavaModel<ALSModel>, IJavaMLWritable, IJavaMLReadable<ALSModel>
    {
        private static readonly string s_className = "org.apache.spark.ml.recommendation.ALSModel";

        /// <summary>
        /// Creates a <see cref="ALSModel"/> with a UID that is used to give the
        /// <see cref="ALSModel"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        /// <param name="rank">rank of the matrix factorization model.</param>
        /// <param name="userFactors">a DataFrame that stores user factors in two columns: id and features.</param>
        /// <param name="itemFactors">a DataFrame that stores item factors in two columns: id and features.</param>
        public ALSModel(string uid, int rank, DataFrame userFactors, DataFrame itemFactors)
            : base(s_className, uid, rank, userFactors, itemFactors)
        {
        }

        internal ALSModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets blockSize value for <see cref="blockSize"/>
        /// </summary>
        /// <param name="blockSize">
        /// block size for stacking input data in matrices. Data is stacked within partitions. If block size is more than remaining data in a partition then it is adjusted to the size of this data.
        /// </param>
        /// <returns> New ALSModel object </returns>
        public ALSModel SetBlockSize(int value) =>
            WrapAsALSModel(Reference.Invoke("setBlockSize", (object)value));
        
        /// <summary>
        /// Sets coldStartStrategy value for <see cref="coldStartStrategy"/>
        /// </summary>
        /// <param name="coldStartStrategy">
        /// strategy for dealing with unknown or new users/items at prediction time. This may be useful in cross-validation or production scenarios, for handling user/item ids the model has not seen in the training data. Supported values: nan,drop.
        /// </param>
        /// <returns> New ALSModel object </returns>
        public ALSModel SetColdStartStrategy(string value) =>
            WrapAsALSModel(Reference.Invoke("setColdStartStrategy", (object)value));
        
        /// <summary>
        /// Sets itemCol value for <see cref="itemCol"/>
        /// </summary>
        /// <param name="itemCol">
        /// column name for item ids. Ids must be within the integer value range.
        /// </param>
        /// <returns> New ALSModel object </returns>
        public ALSModel SetItemCol(string value) =>
            WrapAsALSModel(Reference.Invoke("setItemCol", (object)value));
        
        /// <summary>
        /// Sets predictionCol value for <see cref="predictionCol"/>
        /// </summary>
        /// <param name="predictionCol">
        /// prediction column name
        /// </param>
        /// <returns> New ALSModel object </returns>
        public ALSModel SetPredictionCol(string value) =>
            WrapAsALSModel(Reference.Invoke("setPredictionCol", (object)value));
        
        /// <summary>
        /// Sets userCol value for <see cref="userCol"/>
        /// </summary>
        /// <param name="userCol">
        /// column name for user ids. Ids must be within the integer value range.
        /// </param>
        /// <returns> New ALSModel object </returns>
        public ALSModel SetUserCol(string value) =>
            WrapAsALSModel(Reference.Invoke("setUserCol", (object)value));

        
        /// <summary>
        /// Gets blockSize value for <see cref="blockSize"/>
        /// </summary>
        /// <returns>
        /// blockSize: block size for stacking input data in matrices. Data is stacked within partitions. If block size is more than remaining data in a partition then it is adjusted to the size of this data.
        /// </returns>
        public int GetBlockSize() =>
            (int)Reference.Invoke("getBlockSize");
        
        
        /// <summary>
        /// Gets coldStartStrategy value for <see cref="coldStartStrategy"/>
        /// </summary>
        /// <returns>
        /// coldStartStrategy: strategy for dealing with unknown or new users/items at prediction time. This may be useful in cross-validation or production scenarios, for handling user/item ids the model has not seen in the training data. Supported values: nan,drop.
        /// </returns>
        public string GetColdStartStrategy() =>
            (string)Reference.Invoke("getColdStartStrategy");
        
        
        /// <summary>
        /// Gets itemCol value for <see cref="itemCol"/>
        /// </summary>
        /// <returns>
        /// itemCol: column name for item ids. Ids must be within the integer value range.
        /// </returns>
        public string GetItemCol() =>
            (string)Reference.Invoke("getItemCol");
        
        
        /// <summary>
        /// Gets predictionCol value for <see cref="predictionCol"/>
        /// </summary>
        /// <returns>
        /// predictionCol: prediction column name
        /// </returns>
        public string GetPredictionCol() =>
            (string)Reference.Invoke("getPredictionCol");
        
        
        /// <summary>
        /// Gets userCol value for <see cref="userCol"/>
        /// </summary>
        /// <returns>
        /// userCol: column name for user ids. Ids must be within the integer value range.
        /// </returns>
        public string GetUserCol() =>
            (string)Reference.Invoke("getUserCol");

        
        /// <summary>
        /// Loads the <see cref="ALSModel"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="ALSModel"/> was saved to</param>
        /// <returns>New <see cref="ALSModel"/> object, loaded from path.</returns>
        public static ALSModel Load(string path) => WrapAsALSModel(
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
        /// <returns>an <see cref="JavaMLReader&lt;ALSModel&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<ALSModel> Read() =>
            new JavaMLReader<ALSModel>((JvmObjectReference)Reference.Invoke("read"));

        private static ALSModel WrapAsALSModel(object obj) =>
            new ALSModel((JvmObjectReference)obj);

        
    }
}

        