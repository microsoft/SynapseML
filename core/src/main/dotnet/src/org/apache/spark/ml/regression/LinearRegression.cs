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
using Microsoft.Spark.ML.Regression;

namespace Microsoft.Spark.ML.Regression
{
    /// <summary>
    /// <see cref="LinearRegression"/> implements LinearRegression
    /// </summary>
    public class LinearRegression : JavaEstimator<LinearRegressionModel>, IJavaMLWritable, IJavaMLReadable<LinearRegression>
    {
        private static readonly string s_className = "org.apache.spark.ml.regression.LinearRegression";

        /// <summary>
        /// Creates a <see cref="LinearRegression"/> without any parameters.
        /// </summary>
        public LinearRegression() : base(s_className)
        {
        }

        /// <summary>
        /// Creates a <see cref="LinearRegression"/> with a UID that is used to give the
        /// <see cref="LinearRegression"/> a unique ID.
        /// </summary>
        /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        public LinearRegression(string uid) : base(s_className, uid)
        {
        }

        internal LinearRegression(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets aggregationDepth value for <see cref="aggregationDepth"/>
        /// </summary>
        /// <param name="aggregationDepth">
        /// suggested depth for treeAggregate (>= 2)
        /// </param>
        /// <returns> New LinearRegression object </returns>
        public LinearRegression SetAggregationDepth(int value) =>
            WrapAsLinearRegression(Reference.Invoke("setAggregationDepth", (object)value));

        /// <summary>
        /// Sets elasticNetParam value for <see cref="elasticNetParam"/>
        /// </summary>
        /// <param name="elasticNetParam">
        /// the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty
        /// </param>
        /// <returns> New LinearRegression object </returns>
        public LinearRegression SetElasticNetParam(double value) =>
            WrapAsLinearRegression(Reference.Invoke("setElasticNetParam", (object)value));

        /// <summary>
        /// Sets epsilon value for <see cref="epsilon"/>
        /// </summary>
        /// <param name="epsilon">
        /// The shape parameter to control the amount of robustness. Must be > 1.0.
        /// </param>
        /// <returns> New LinearRegression object </returns>
        public LinearRegression SetEpsilon(double value) =>
            WrapAsLinearRegression(Reference.Invoke("setEpsilon", (object)value));

        /// <summary>
        /// Sets featuresCol value for <see cref="featuresCol"/>
        /// </summary>
        /// <param name="featuresCol">
        /// features column name
        /// </param>
        /// <returns> New LinearRegression object </returns>
        public LinearRegression SetFeaturesCol(string value) =>
            WrapAsLinearRegression(Reference.Invoke("setFeaturesCol", (object)value));

        /// <summary>
        /// Sets fitIntercept value for <see cref="fitIntercept"/>
        /// </summary>
        /// <param name="fitIntercept">
        /// whether to fit an intercept term
        /// </param>
        /// <returns> New LinearRegression object </returns>
        public LinearRegression SetFitIntercept(bool value) =>
            WrapAsLinearRegression(Reference.Invoke("setFitIntercept", (object)value));

        /// <summary>
        /// Sets labelCol value for <see cref="labelCol"/>
        /// </summary>
        /// <param name="labelCol">
        /// label column name
        /// </param>
        /// <returns> New LinearRegression object </returns>
        public LinearRegression SetLabelCol(string value) =>
            WrapAsLinearRegression(Reference.Invoke("setLabelCol", (object)value));

        /// <summary>
        /// Sets loss value for <see cref="loss"/>
        /// </summary>
        /// <param name="loss">
        /// The loss function to be optimized. Supported options: squaredError, huber. (Default squaredError)
        /// </param>
        /// <returns> New LinearRegression object </returns>
        public LinearRegression SetLoss(string value) =>
            WrapAsLinearRegression(Reference.Invoke("setLoss", (object)value));

        /// <summary>
        /// Sets maxBlockSizeInMB value for <see cref="maxBlockSizeInMB"/>
        /// </summary>
        /// <param name="maxBlockSizeInMB">
        /// Maximum memory in MB for stacking input data into blocks. Data is stacked within partitions. If more than remaining data size in a partition then it is adjusted to the data size. Default 0.0 represents choosing optimal value, depends on specific algorithm. Must be >= 0.
        /// </param>
        /// <returns> New LinearRegression object </returns>
        public LinearRegression SetMaxBlockSizeInMB(double value) =>
            WrapAsLinearRegression(Reference.Invoke("setMaxBlockSizeInMB", (object)value));

        /// <summary>
        /// Sets maxIter value for <see cref="maxIter"/>
        /// </summary>
        /// <param name="maxIter">
        /// maximum number of iterations (>= 0)
        /// </param>
        /// <returns> New LinearRegression object </returns>
        public LinearRegression SetMaxIter(int value) =>
            WrapAsLinearRegression(Reference.Invoke("setMaxIter", (object)value));

        /// <summary>
        /// Sets predictionCol value for <see cref="predictionCol"/>
        /// </summary>
        /// <param name="predictionCol">
        /// prediction column name
        /// </param>
        /// <returns> New LinearRegression object </returns>
        public LinearRegression SetPredictionCol(string value) =>
            WrapAsLinearRegression(Reference.Invoke("setPredictionCol", (object)value));

        /// <summary>
        /// Sets regParam value for <see cref="regParam"/>
        /// </summary>
        /// <param name="regParam">
        /// regularization parameter (>= 0)
        /// </param>
        /// <returns> New LinearRegression object </returns>
        public LinearRegression SetRegParam(double value) =>
            WrapAsLinearRegression(Reference.Invoke("setRegParam", (object)value));

        /// <summary>
        /// Sets solver value for <see cref="solver"/>
        /// </summary>
        /// <param name="solver">
        /// The solver algorithm for optimization. Supported options: auto, normal, l-bfgs. (Default auto)
        /// </param>
        /// <returns> New LinearRegression object </returns>
        public LinearRegression SetSolver(string value) =>
            WrapAsLinearRegression(Reference.Invoke("setSolver", (object)value));

        /// <summary>
        /// Sets standardization value for <see cref="standardization"/>
        /// </summary>
        /// <param name="standardization">
        /// whether to standardize the training features before fitting the model
        /// </param>
        /// <returns> New LinearRegression object </returns>
        public LinearRegression SetStandardization(bool value) =>
            WrapAsLinearRegression(Reference.Invoke("setStandardization", (object)value));

        /// <summary>
        /// Sets tol value for <see cref="tol"/>
        /// </summary>
        /// <param name="tol">
        /// the convergence tolerance for iterative algorithms (>= 0)
        /// </param>
        /// <returns> New LinearRegression object </returns>
        public LinearRegression SetTol(double value) =>
            WrapAsLinearRegression(Reference.Invoke("setTol", (object)value));

        /// <summary>
        /// Sets weightCol value for <see cref="weightCol"/>
        /// </summary>
        /// <param name="weightCol">
        /// weight column name. If this is not set or empty, we treat all instance weights as 1.0
        /// </param>
        /// <returns> New LinearRegression object </returns>
        public LinearRegression SetWeightCol(string value) =>
            WrapAsLinearRegression(Reference.Invoke("setWeightCol", (object)value));


        /// <summary>
        /// Gets aggregationDepth value for <see cref="aggregationDepth"/>
        /// </summary>
        /// <returns>
        /// aggregationDepth: suggested depth for treeAggregate (>= 2)
        /// </returns>
        public int GetAggregationDepth() =>
            (int)Reference.Invoke("getAggregationDepth");


        /// <summary>
        /// Gets elasticNetParam value for <see cref="elasticNetParam"/>
        /// </summary>
        /// <returns>
        /// elasticNetParam: the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty
        /// </returns>
        public double GetElasticNetParam() =>
            (double)Reference.Invoke("getElasticNetParam");


        /// <summary>
        /// Gets epsilon value for <see cref="epsilon"/>
        /// </summary>
        /// <returns>
        /// epsilon: The shape parameter to control the amount of robustness. Must be > 1.0.
        /// </returns>
        public double GetEpsilon() =>
            (double)Reference.Invoke("getEpsilon");


        /// <summary>
        /// Gets featuresCol value for <see cref="featuresCol"/>
        /// </summary>
        /// <returns>
        /// featuresCol: features column name
        /// </returns>
        public string GetFeaturesCol() =>
            (string)Reference.Invoke("getFeaturesCol");


        /// <summary>
        /// Gets fitIntercept value for <see cref="fitIntercept"/>
        /// </summary>
        /// <returns>
        /// fitIntercept: whether to fit an intercept term
        /// </returns>
        public bool GetFitIntercept() =>
            (bool)Reference.Invoke("getFitIntercept");


        /// <summary>
        /// Gets labelCol value for <see cref="labelCol"/>
        /// </summary>
        /// <returns>
        /// labelCol: label column name
        /// </returns>
        public string GetLabelCol() =>
            (string)Reference.Invoke("getLabelCol");


        /// <summary>
        /// Gets loss value for <see cref="loss"/>
        /// </summary>
        /// <returns>
        /// loss: The loss function to be optimized. Supported options: squaredError, huber. (Default squaredError)
        /// </returns>
        public string GetLoss() =>
            (string)Reference.Invoke("getLoss");


        /// <summary>
        /// Gets maxBlockSizeInMB value for <see cref="maxBlockSizeInMB"/>
        /// </summary>
        /// <returns>
        /// maxBlockSizeInMB: Maximum memory in MB for stacking input data into blocks. Data is stacked within partitions. If more than remaining data size in a partition then it is adjusted to the data size. Default 0.0 represents choosing optimal value, depends on specific algorithm. Must be >= 0.
        /// </returns>
        public double GetMaxBlockSizeInMB() =>
            (double)Reference.Invoke("getMaxBlockSizeInMB");


        /// <summary>
        /// Gets maxIter value for <see cref="maxIter"/>
        /// </summary>
        /// <returns>
        /// maxIter: maximum number of iterations (>= 0)
        /// </returns>
        public int GetMaxIter() =>
            (int)Reference.Invoke("getMaxIter");


        /// <summary>
        /// Gets predictionCol value for <see cref="predictionCol"/>
        /// </summary>
        /// <returns>
        /// predictionCol: prediction column name
        /// </returns>
        public string GetPredictionCol() =>
            (string)Reference.Invoke("getPredictionCol");


        /// <summary>
        /// Gets regParam value for <see cref="regParam"/>
        /// </summary>
        /// <returns>
        /// regParam: regularization parameter (>= 0)
        /// </returns>
        public double GetRegParam() =>
            (double)Reference.Invoke("getRegParam");


        /// <summary>
        /// Gets solver value for <see cref="solver"/>
        /// </summary>
        /// <returns>
        /// solver: The solver algorithm for optimization. Supported options: auto, normal, l-bfgs. (Default auto)
        /// </returns>
        public string GetSolver() =>
            (string)Reference.Invoke("getSolver");


        /// <summary>
        /// Gets standardization value for <see cref="standardization"/>
        /// </summary>
        /// <returns>
        /// standardization: whether to standardize the training features before fitting the model
        /// </returns>
        public bool GetStandardization() =>
            (bool)Reference.Invoke("getStandardization");


        /// <summary>
        /// Gets tol value for <see cref="tol"/>
        /// </summary>
        /// <returns>
        /// tol: the convergence tolerance for iterative algorithms (>= 0)
        /// </returns>
        public double GetTol() =>
            (double)Reference.Invoke("getTol");


        /// <summary>
        /// Gets weightCol value for <see cref="weightCol"/>
        /// </summary>
        /// <returns>
        /// weightCol: weight column name. If this is not set or empty, we treat all instance weights as 1.0
        /// </returns>
        public string GetWeightCol() =>
            (string)Reference.Invoke("getWeightCol");

        /// <summary>Fits a model to the input data.</summary>
        /// <param name="dataset">The <see cref="DataFrame"/> to fit the model to.</param>
        /// <returns><see cref="LinearRegressionModel"/></returns>
        override public LinearRegressionModel Fit(DataFrame dataset) =>
            new LinearRegressionModel(
                (JvmObjectReference)Reference.Invoke("fit", dataset));

        /// <summary>
        /// Loads the <see cref="LinearRegression"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="LinearRegression"/> was saved to</param>
        /// <returns>New <see cref="LinearRegression"/> object, loaded from path.</returns>
        public static LinearRegression Load(string path) => WrapAsLinearRegression(
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
        /// <returns>an <see cref="JavaMLReader&lt;LinearRegression&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<LinearRegression> Read() =>
            new JavaMLReader<LinearRegression>((JvmObjectReference)Reference.Invoke("read"));

        private static LinearRegression WrapAsLinearRegression(object obj) =>
            new LinearRegression((JvmObjectReference)obj);


    }
}
