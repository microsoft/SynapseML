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


namespace Microsoft.Spark.ML.Classification
{
    /// <summary>
    /// <see cref="LogisticRegressionModel"/> implements LogisticRegressionModel
    /// </summary>
    public class LogisticRegressionModel : JavaModel<LogisticRegressionModel>, IJavaMLWritable, IJavaMLReadable<LogisticRegressionModel>
    {
        private static readonly string s_className = "org.apache.spark.ml.classification.LogisticRegressionModel";

        // // TODO: support this after constructing Vector and Matrix class in .NET
        // /// <summary>
        // /// Creates a <see cref="LogisticRegressionModel"/> with a UID that is used to give the
        // /// <see cref="LogisticRegressionModel"/> a unique ID.
        // /// </summary>
        // /// <param name="uid">An immutable unique ID for the object and its derivatives.</param>
        // public LogisticRegressionModel(string uid, Matrix coefficientMatrix, Vector interceptVector, int numClasses, bool isMultinomial)
        //     : this(SparkEnvironment.JvmBridge.CallConstructor(s_className, uid))
        // {
        // }

        internal LogisticRegressionModel(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        /// <summary>
        /// Sets aggregationDepth value for <see cref="aggregationDepth"/>
        /// </summary>
        /// <param name="aggregationDepth">
        /// suggested depth for treeAggregate (>= 2)
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetAggregationDepth(int value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setAggregationDepth", (object)value));

        /// <summary>
        /// Sets elasticNetParam value for <see cref="elasticNetParam"/>
        /// </summary>
        /// <param name="elasticNetParam">
        /// the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetElasticNetParam(double value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setElasticNetParam", (object)value));

        /// <summary>
        /// Sets family value for <see cref="family"/>
        /// </summary>
        /// <param name="family">
        /// The name of family which is a description of the label distribution to be used in the model. Supported options: auto, binomial, multinomial.
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetFamily(string value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setFamily", (object)value));

        /// <summary>
        /// Sets featuresCol value for <see cref="featuresCol"/>
        /// </summary>
        /// <param name="featuresCol">
        /// features column name
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetFeaturesCol(string value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setFeaturesCol", (object)value));

        /// <summary>
        /// Sets fitIntercept value for <see cref="fitIntercept"/>
        /// </summary>
        /// <param name="fitIntercept">
        /// whether to fit an intercept term
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetFitIntercept(bool value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setFitIntercept", (object)value));

        /// <summary>
        /// Sets labelCol value for <see cref="labelCol"/>
        /// </summary>
        /// <param name="labelCol">
        /// label column name
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetLabelCol(string value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setLabelCol", (object)value));

        /// <summary>
        /// Sets lowerBoundsOnCoefficients value for <see cref="lowerBoundsOnCoefficients"/>
        /// </summary>
        /// <param name="lowerBoundsOnCoefficients">
        /// The lower bounds on coefficients if fitting under bound constrained optimization.
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetLowerBoundsOnCoefficients(object value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setLowerBoundsOnCoefficients", (object)value));

        /// <summary>
        /// Sets lowerBoundsOnIntercepts value for <see cref="lowerBoundsOnIntercepts"/>
        /// </summary>
        /// <param name="lowerBoundsOnIntercepts">
        /// The lower bounds on intercepts if fitting under bound constrained optimization.
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetLowerBoundsOnIntercepts(object value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setLowerBoundsOnIntercepts", (object)value));

        /// <summary>
        /// Sets maxBlockSizeInMB value for <see cref="maxBlockSizeInMB"/>
        /// </summary>
        /// <param name="maxBlockSizeInMB">
        /// Maximum memory in MB for stacking input data into blocks. Data is stacked within partitions. If more than remaining data size in a partition then it is adjusted to the data size. Default 0.0 represents choosing optimal value, depends on specific algorithm. Must be >= 0.
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetMaxBlockSizeInMB(double value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setMaxBlockSizeInMB", (object)value));

        /// <summary>
        /// Sets maxIter value for <see cref="maxIter"/>
        /// </summary>
        /// <param name="maxIter">
        /// maximum number of iterations (>= 0)
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetMaxIter(int value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setMaxIter", (object)value));

        /// <summary>
        /// Sets predictionCol value for <see cref="predictionCol"/>
        /// </summary>
        /// <param name="predictionCol">
        /// prediction column name
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetPredictionCol(string value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setPredictionCol", (object)value));

        /// <summary>
        /// Sets probabilityCol value for <see cref="probabilityCol"/>
        /// </summary>
        /// <param name="probabilityCol">
        /// Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetProbabilityCol(string value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setProbabilityCol", (object)value));

        /// <summary>
        /// Sets rawPredictionCol value for <see cref="rawPredictionCol"/>
        /// </summary>
        /// <param name="rawPredictionCol">
        /// raw prediction (a.k.a. confidence) column name
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetRawPredictionCol(string value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setRawPredictionCol", (object)value));

        /// <summary>
        /// Sets regParam value for <see cref="regParam"/>
        /// </summary>
        /// <param name="regParam">
        /// regularization parameter (>= 0)
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetRegParam(double value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setRegParam", (object)value));

        /// <summary>
        /// Sets standardization value for <see cref="standardization"/>
        /// </summary>
        /// <param name="standardization">
        /// whether to standardize the training features before fitting the model
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetStandardization(bool value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setStandardization", (object)value));

        /// <summary>
        /// Sets threshold value for <see cref="threshold"/>
        /// </summary>
        /// <param name="threshold">
        /// threshold in binary classification prediction, in range [0, 1]
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetThreshold(double value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setThreshold", (object)value));

        /// <summary>
        /// Sets thresholds value for <see cref="thresholds"/>
        /// </summary>
        /// <param name="thresholds">
        /// Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetThresholds(double[] value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setThresholds", (object)value));

        /// <summary>
        /// Sets tol value for <see cref="tol"/>
        /// </summary>
        /// <param name="tol">
        /// the convergence tolerance for iterative algorithms (>= 0)
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetTol(double value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setTol", (object)value));

        /// <summary>
        /// Sets upperBoundsOnCoefficients value for <see cref="upperBoundsOnCoefficients"/>
        /// </summary>
        /// <param name="upperBoundsOnCoefficients">
        /// The upper bounds on coefficients if fitting under bound constrained optimization.
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetUpperBoundsOnCoefficients(object value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setUpperBoundsOnCoefficients", (object)value));

        /// <summary>
        /// Sets upperBoundsOnIntercepts value for <see cref="upperBoundsOnIntercepts"/>
        /// </summary>
        /// <param name="upperBoundsOnIntercepts">
        /// The upper bounds on intercepts if fitting under bound constrained optimization.
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetUpperBoundsOnIntercepts(object value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setUpperBoundsOnIntercepts", (object)value));

        /// <summary>
        /// Sets weightCol value for <see cref="weightCol"/>
        /// </summary>
        /// <param name="weightCol">
        /// weight column name. If this is not set or empty, we treat all instance weights as 1.0
        /// </param>
        /// <returns> New LogisticRegressionModel object </returns>
        public LogisticRegressionModel SetWeightCol(string value) =>
            WrapAsLogisticRegressionModel(Reference.Invoke("setWeightCol", (object)value));


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
        /// Gets family value for <see cref="family"/>
        /// </summary>
        /// <returns>
        /// family: The name of family which is a description of the label distribution to be used in the model. Supported options: auto, binomial, multinomial.
        /// </returns>
        public string GetFamily() =>
            (string)Reference.Invoke("getFamily");


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
        /// Gets lowerBoundsOnCoefficients value for <see cref="lowerBoundsOnCoefficients"/>
        /// </summary>
        /// <returns>
        /// lowerBoundsOnCoefficients: The lower bounds on coefficients if fitting under bound constrained optimization.
        /// </returns>
        public object GetLowerBoundsOnCoefficients() =>
            (object)Reference.Invoke("getLowerBoundsOnCoefficients");


        /// <summary>
        /// Gets lowerBoundsOnIntercepts value for <see cref="lowerBoundsOnIntercepts"/>
        /// </summary>
        /// <returns>
        /// lowerBoundsOnIntercepts: The lower bounds on intercepts if fitting under bound constrained optimization.
        /// </returns>
        public object GetLowerBoundsOnIntercepts() =>
            (object)Reference.Invoke("getLowerBoundsOnIntercepts");


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
        /// Gets probabilityCol value for <see cref="probabilityCol"/>
        /// </summary>
        /// <returns>
        /// probabilityCol: Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities
        /// </returns>
        public string GetProbabilityCol() =>
            (string)Reference.Invoke("getProbabilityCol");


        /// <summary>
        /// Gets rawPredictionCol value for <see cref="rawPredictionCol"/>
        /// </summary>
        /// <returns>
        /// rawPredictionCol: raw prediction (a.k.a. confidence) column name
        /// </returns>
        public string GetRawPredictionCol() =>
            (string)Reference.Invoke("getRawPredictionCol");


        /// <summary>
        /// Gets regParam value for <see cref="regParam"/>
        /// </summary>
        /// <returns>
        /// regParam: regularization parameter (>= 0)
        /// </returns>
        public double GetRegParam() =>
            (double)Reference.Invoke("getRegParam");


        /// <summary>
        /// Gets standardization value for <see cref="standardization"/>
        /// </summary>
        /// <returns>
        /// standardization: whether to standardize the training features before fitting the model
        /// </returns>
        public bool GetStandardization() =>
            (bool)Reference.Invoke("getStandardization");


        /// <summary>
        /// Gets threshold value for <see cref="threshold"/>
        /// </summary>
        /// <returns>
        /// threshold: threshold in binary classification prediction, in range [0, 1]
        /// </returns>
        public double GetThreshold() =>
            (double)Reference.Invoke("getThreshold");


        /// <summary>
        /// Gets thresholds value for <see cref="thresholds"/>
        /// </summary>
        /// <returns>
        /// thresholds: Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold
        /// </returns>
        public double[] GetThresholds() =>
            (double[])Reference.Invoke("getThresholds");


        /// <summary>
        /// Gets tol value for <see cref="tol"/>
        /// </summary>
        /// <returns>
        /// tol: the convergence tolerance for iterative algorithms (>= 0)
        /// </returns>
        public double GetTol() =>
            (double)Reference.Invoke("getTol");


        /// <summary>
        /// Gets upperBoundsOnCoefficients value for <see cref="upperBoundsOnCoefficients"/>
        /// </summary>
        /// <returns>
        /// upperBoundsOnCoefficients: The upper bounds on coefficients if fitting under bound constrained optimization.
        /// </returns>
        public object GetUpperBoundsOnCoefficients() =>
            (object)Reference.Invoke("getUpperBoundsOnCoefficients");


        /// <summary>
        /// Gets upperBoundsOnIntercepts value for <see cref="upperBoundsOnIntercepts"/>
        /// </summary>
        /// <returns>
        /// upperBoundsOnIntercepts: The upper bounds on intercepts if fitting under bound constrained optimization.
        /// </returns>
        public object GetUpperBoundsOnIntercepts() =>
            (object)Reference.Invoke("getUpperBoundsOnIntercepts");


        /// <summary>
        /// Gets weightCol value for <see cref="weightCol"/>
        /// </summary>
        /// <returns>
        /// weightCol: weight column name. If this is not set or empty, we treat all instance weights as 1.0
        /// </returns>
        public string GetWeightCol() =>
            (string)Reference.Invoke("getWeightCol");


        /// <summary>
        /// Loads the <see cref="LogisticRegressionModel"/> that was previously saved using Save(string).
        /// </summary>
        /// <param name="path">The path the previous <see cref="LogisticRegressionModel"/> was saved to</param>
        /// <returns>New <see cref="LogisticRegressionModel"/> object, loaded from path.</returns>
        public static LogisticRegressionModel Load(string path) => WrapAsLogisticRegressionModel(
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
        /// <returns>an <see cref="JavaMLReader&lt;LogisticRegressionModel&gt;"/> instance for this ML instance.</returns>
        public JavaMLReader<LogisticRegressionModel> Read() =>
            new JavaMLReader<LogisticRegressionModel>((JvmObjectReference)Reference.Invoke("read"));

        private static LogisticRegressionModel WrapAsLogisticRegressionModel(object obj) =>
            new LogisticRegressionModel((JvmObjectReference)obj);


    }
}
