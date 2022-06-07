// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

using System;
using System.IO;
using System.Collections.Generic;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.ML.Feature;
using Microsoft.Spark.ML.Feature.Param;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Xunit;
using SynapseMLtest.Utils;
// using SynapseMLtest.DotnetUtils;
using Synapse.ML.Cognitive;
using Synapse.ML.Recommendation;
using Synapse.ML.Featurize.Text;
// using test;

using Synapse.ML.Automl;
using Microsoft.Spark.ML.Recommendation;
using Microsoft.Spark.ML;
using SynapseML.Dotnet.Utils;
using Synapse.ML.Io.Http;
using Synapse.ML.Stages;
using Synapse.ML.Featurize;
using Synapse.ML.Opencv;
using Synapse.ML.Lightgbm;
using Synapse.ML.Explainers;
using Synapse.ML.LightGBM.Param;

using System.Reflection;

namespace SynapseMLtest.Cognitive
{

    public class FeaturesFixture
    {
        public FeaturesFixture()
        {
            sparkFixture = new SparkFixture();
        }

        public SparkFixture sparkFixture { get; private set; }
    }

    public class FeaturesTests : IClassFixture<FeaturesFixture>
    {
        private readonly SparkSession _spark;
        private readonly IJvmBridge _jvm;
        public FeaturesTests(FeaturesFixture fixture)
        {
            _spark = fixture.sparkFixture.Spark;
            _jvm = fixture.sparkFixture.Jvm;
        }

        [Fact]
        public void TestDetectLastAnomalyConstructor0()
        {
            string TestDataDir = "D:\\repos\\SynapseML\\cognitive\\target\\scala-2.12\\generated\\test-data\\dotnet\\DetectLastAnomalySuite";

            void AssertCorrespondence(DetectLastAnomaly model, string name, int num)
            {
                model.Write().Overwrite().Save(Path.Combine(TestDataDir, name));
                _jvm.CallStaticJavaMethod("com.microsoft.azure.synapse.ml.core.utils.ModelEquality",
                    "assertEqual", "com.microsoft.azure.synapse.ml.cognitive.DetectLastAnomaly", Path.Combine(TestDataDir, name),
                    Path.Combine(TestDataDir, String.Format("model-{0}.model", num)));
            }

            string path = Path.Combine(TestDataDir, "model-0.model", "complexParams", "handler");
            var handlerParam = _jvm.CallStaticJavaMethod("org.apache.spark.ml.param.UDFParam", "loadForTest", _spark, path);

            var model = new DetectLastAnomaly()
                .SetTimeout(60.0)
                .SetHandler(handlerParam)
                .SetGranularity("monthly")
                .SetUrl("https://westus2.api.cognitive.microsoft.com//anomalydetector/v1.1-preview.1/timeseries/last/detect")
                .SetOutputCol("anomalies")
                .SetSeriesCol("inputs")
                .SetErrorCol("errors")
                .SetConcurrency(1)
                .SetSubscriptionKey("4bda24390e4341f89623b7706e7c550f");



            AssertCorrespondence(model, "dotnet-constructor-model-0.model", 0);


        }

        [Fact]
        public void TestAnalyzeBusinessCardsConstructor0()
        {
            string TestDataDir = "D:\\repos\\SynapseML\\cognitive\\target\\scala-2.12\\generated\\test-data\\dotnet\\AnalyzeBusinessCardsSuite";

            void AssertCorrespondence(Synapse.ML.Cognitive.AnalyzeBusinessCards model, string name, int num)
            {
                model.Write().Overwrite().Save(Path.Combine(TestDataDir, name));
                _jvm.CallStaticJavaMethod("com.microsoft.azure.synapse.ml.core.utils.ModelEquality",
                    "assertEqual", "com.microsoft.azure.synapse.ml.cognitive.AnalyzeBusinessCards", Path.Combine(TestDataDir, name),
                    Path.Combine(TestDataDir, String.Format("model-{0}.model", num)));
            }
            
            
            
            var model = new Synapse.ML.Cognitive.AnalyzeBusinessCards()
                .SetOutputCol("businessCards")
                .SetConcurrency(5)
                .SetPollingDelay(300)
                .SetBackoffs(new int[]
                    {100,500,1000})
                .SetTimeout(60.0)
                .SetUrl("https://eastus.api.cognitive.microsoft.com//formrecognizer/v2.1/prebuilt/businessCard/analyze")
                .SetSuppressMaxRetriesExceededException(false)
                .SetImageUrlCol("source")
                .SetSubscriptionKey("df74b0018d394ca0ab2173f3623ca7a1")
                .SetInitialPollingDelay(300)
                .SetMaxPollingRetries(1000)
                .SetErrorCol("AnalyzeBusinessCards_5e1b587c0b97_error");
            

        
            AssertCorrespondence(model, "dotnet-constructor-model-0.model", 0);
        
            
        }

        [Fact]
        public void TestDataConversionConstructor0()
        {
            string TestDataDir = "D:\\repos\\SynapseML\\core\\target\\scala-2.12\\generated\\test-data\\dotnet\\VerifyDataConversions";

            void AssertCorrespondence(Synapse.ML.Featurize.DataConversion model, string name, int num)
            {
                model.Write().Overwrite().Save(Path.Combine(TestDataDir, name));
                _jvm.CallStaticJavaMethod("com.microsoft.azure.synapse.ml.core.utils.ModelEquality",
                    "assertEqual", "com.microsoft.azure.synapse.ml.featurize.DataConversion", Path.Combine(TestDataDir, name),
                    Path.Combine(TestDataDir, String.Format("model-{0}.model", num)));
            }
            
            var model = new Synapse.ML.Featurize.DataConversion()
                .SetDateTimeFormat("yyyy-MM-dd HH:mm:ss.SSS")
                .SetConvertTo("date")
                .SetCols(new string[]
                    {"Col0"});
        
            AssertCorrespondence(model, "dotnet-constructor-model-0.model", 0);
        
            
        }

        [Fact]
        public void TestEstimatorParamLoad()
        {
            string TestDataDir = "D:\\repos\\SynapseML\\core\\target\\scala-2.12\\generated\\test-data\\dotnet\\RankingAdapterSpec";

            void AssertCorrespondence(Synapse.ML.Recommendation.RankingAdapter model, string name, int num)
            {
                model.Write().Overwrite().Save(Path.Combine(TestDataDir, name));
                _jvm.CallStaticJavaMethod("com.microsoft.azure.synapse.ml.core.utils.ModelEquality",
                    "assertEqual", "com.microsoft.azure.synapse.ml.recommendation.RankingAdapter", Path.Combine(TestDataDir, name),
                    Path.Combine(TestDataDir, String.Format("model-{0}.model", num)));
            }
            
            
            var recommenderLoad = Pipeline.Load(
                Path.Combine(TestDataDir, "model-0.model", "complexParams", "recommender"));
            var recommenderModel = (ALS)recommenderLoad.GetStages()[0];
            
            
            var model = new Synapse.ML.Recommendation.RankingAdapter()
                .SetLabelCol("label")
                .SetMinRatingsPerUser(1)
                .SetK(3)
                .SetMinRatingsPerItem(1)
                .SetMode("allUsers")
                .SetRecommender(recommenderModel);
            

        
            AssertCorrespondence(model, "dotnet-constructor-model-0.model", 0);
        }

        [Fact]
        public void TestTransformerParamLoad()
        {

            string TestDataDir = "D:\\repos\\SynapseML\\core\\target\\scala-2.12\\generated\\test-data\\dotnet\\RankingAdapterModelSpec";

            void AssertCorrespondence(Synapse.ML.Recommendation.RankingAdapterModel model, string name, int num)
            {
                model.Write().Overwrite().Save(Path.Combine(TestDataDir, name));
                _jvm.CallStaticJavaMethod("com.microsoft.azure.synapse.ml.core.utils.ModelEquality",
                    "assertEqual", "com.microsoft.azure.synapse.ml.recommendation.RankingAdapterModel", Path.Combine(TestDataDir, name),
                    Path.Combine(TestDataDir, String.Format("model-{0}.model", num)));
            }
            
            
            var recommenderModelLoad = Microsoft.Spark.ML.Feature.Pipeline.Load(
                Path.Combine(TestDataDir, "model-0.model", "complexParams", "recommenderModel"));
            var recommenderModelModel = (JavaTransformer)recommenderModelLoad.GetStages()[0];
            
            
            var model = new Synapse.ML.Recommendation.RankingAdapterModel()
                .SetRecommenderModel(recommenderModelModel)
                .SetMode("allUsers")
                .SetMinRatingsPerItem(1)
                .SetUserCol("customerID")
                .SetRatingCol("rating")
                .SetLabelCol("label")
                .SetItemCol("itemID")
                .SetMinRatingsPerUser(1)
                .SetK(3);

        
            AssertCorrespondence(model, "dotnet-constructor-model-0.model", 0);
        }

        [Fact]
        public void TestRankingTrainValidationSplitConstructor0()
        {

            string TestDataDir = "D:\\repos\\SynapseML\\dotnetClasses\\generated\\test-data\\dotnet\\RankingTrainValidationSplitSpec";

            void AssertCorrespondence(Synapse.ML.Recommendation.RankingTrainValidationSplit model, string name, int num)
            {
                model.Write().Overwrite().Save(Path.Combine(TestDataDir, name));
                _jvm.CallStaticJavaMethod("com.microsoft.azure.synapse.ml.core.utils.ModelEquality",
                    "assertEqual", "com.microsoft.azure.synapse.ml.recommendation.RankingTrainValidationSplit", Path.Combine(TestDataDir, name),
                    Path.Combine(TestDataDir, String.Format("model-{0}.model", num)));
            }
            
            var estimatorParamMapsParamLoaded = (JvmObjectReference[])_jvm.CallStaticJavaMethod(
                "org.apache.spark.ml.param.ArrayParamMapParam",
                "loadForTest",
                _spark,
                Path.Combine(TestDataDir, "model-0.model", "complexParams", "estimatorParamMaps"));
            var estimatorParamMapsParam = new ParamMap[estimatorParamMapsParamLoaded.Length];
            for (int i = 0; i < estimatorParamMapsParamLoaded.Length; i++)
            {
                estimatorParamMapsParam[i] = new ParamMap(estimatorParamMapsParamLoaded[i]);
            }
            
            var estimatorLoaded = Pipeline.Load(
                Path.Combine(TestDataDir, "model-0.model", "complexParams", "estimator"));
            var estimatorModel = (ALS)estimatorLoaded.GetStages()[0];
            
            var evaluatorParamLoaded = (JvmObjectReference)_jvm.CallStaticJavaMethod(
                "org.apache.spark.ml.param.EvaluatorParam",
                "loadForTest",
                _spark,
                Path.Combine(TestDataDir, "model-0.model", "complexParams", "evaluator"));
            var evaluatorParam = new RankingEvaluator(evaluatorParamLoaded);
            
            var model = new Synapse.ML.Recommendation.RankingTrainValidationSplit()
                .SetEstimatorParamMaps(estimatorParamMapsParam)
                .SetBlockSize(4096)
                .SetFinalStorageLevel("MEMORY_AND_DISK")
                .SetNonnegative(false)
                .SetPredictionCol("prediction")
                .SetEstimator(estimatorModel)
                .SetRegParam(0.1)
                .SetMaxIter(10)
                .SetNumItemBlocks(10)
                .SetImplicitPrefs(false)
                .SetUserCol("customerID")
                .SetMinRatingsI(1)
                .SetNumUserBlocks(10)
                .SetMinRatingsU(1)
                .SetItemCol("itemID")
                .SetTrainRatio(0.8)
                .SetAlpha(1.0)
                .SetColdStartStrategy("nan")
                .SetRatingCol("rating")
                .SetSeed(-492944968)
                .SetEvaluator(evaluatorParam)
                .SetIntermediateStorageLevel("MEMORY_AND_DISK")
                .SetParallelism(1)
                .SetCheckpointInterval(10)
                .SetRank(10);
            

        
            AssertCorrespondence(model, "dotnet-constructor-model-0.model", 0);
        
            
        }

        [Fact]
        public void TestCustomOutputParserConstructor0()
        {

            string TestDataDir = "D:\\repos\\SynapseML\\core\\target\\scala-2.12\\generated\\test-data\\dotnet\\CustomOutputParserSuite";

            void AssertCorrespondence(Synapse.ML.Io.Http.CustomOutputParser model, string name, int num)
            {
                model.Write().Overwrite().Save(Path.Combine(TestDataDir, name));
                _jvm.CallStaticJavaMethod("com.microsoft.azure.synapse.ml.core.utils.ModelEquality",
                    "assertEqual", "com.microsoft.azure.synapse.ml.io.http.CustomOutputParser", Path.Combine(TestDataDir, name),
                    Path.Combine(TestDataDir, String.Format("model-{0}.model", num)));
            }
            
            var udfScalaParam = _jvm.CallStaticJavaMethod(
                "org.apache.spark.ml.param.UDFParam",
                "loadForTest",
                _spark,
                Path.Combine(TestDataDir, "model-0.model", "complexParams", "udfScala"));
            
            var model = new Synapse.ML.Io.Http.CustomOutputParser()
                .SetInputCol("unparsedOutput")
                .SetOutputCol("out")
                .SetUdfScala(udfScalaParam);
            

        
            AssertCorrespondence(model, "dotnet-constructor-model-0.model", 0);
        
            
        }        

        [Fact]
        public void TestCustomInputParserConstructor0()
        {

            string TestDataDir = "D:\\repos\\SynapseML\\core\\target\\scala-2.12\\generated\\test-data\\dotnet\\CustomInputParserSuite";
            
            void AssertCorrespondence(Synapse.ML.Io.Http.CustomInputParser model, string name, int num)
            {
                model.Write().Overwrite().Save(Path.Combine(TestDataDir, name));
                _jvm.CallStaticJavaMethod("com.microsoft.azure.synapse.ml.core.utils.ModelEquality",
                    "assertEqual", "com.microsoft.azure.synapse.ml.io.http.CustomInputParser", Path.Combine(TestDataDir, name),
                    Path.Combine(TestDataDir, String.Format("model-{0}.model", num)));
            }
            
            var udfScalaParam = _jvm.CallStaticJavaMethod(
                "org.apache.spark.ml.param.UDFParam",
                "loadForTest",
                _spark,
                Path.Combine(TestDataDir, "model-0.model", "complexParams", "udfScala"));
            
            var model = new Synapse.ML.Io.Http.CustomInputParser()
                .SetUdfScala(udfScalaParam)
                .SetOutputCol("out")
                .SetInputCol("data");
            

        
            AssertCorrespondence(model, "dotnet-constructor-model-0.model", 0);
        
            
        }


        [Fact]
        public void TestEnsembleByKey()
        {

            var jsonInputParser = new JSONInputParser()
                .SetHeaders(new Dictionary<string, string> { { "v1", "1" } });

            var headers = jsonInputParser.GetHeaders();

            var model = new EnsembleByKey()
                .SetStrategy("mean")
                .SetCollapseGroup(false)
                .SetKeys(new string[]
                    {"label1"})
                .SetVectorDims(new Dictionary<string, int>
                    {{"v1",2}})
                .SetCols(new string[]
                    {"score1"});

            var returnVectorDims = model.GetVectorDims();
        }

        [Fact]
        public void TestSAR()
        {
            var sar = new SAR()
                .SetActivityTimeFormat("yyyy/MM/dd'T'h:mm:ss")
                .SetUserCol("customerID")
                .SetPredictionCol("prediction")
                .SetSeed(-1219638142)
                .SetMaxIter(10)
                .SetColdStartStrategy("nan")
                .SetNonnegative(false)
                .SetIntermediateStorageLevel("MEMORY_AND_DISK")
                .SetSimilarityFunction("jaccard")
                .SetRank(10)
                .SetImplicitPrefs(false)
                .SetTimeCol("time")
                .SetNumUserBlocks(10)
                .SetNumItemBlocks(10)
                .SetSupportThreshold(4)
                .SetFinalStorageLevel("MEMORY_AND_DISK")
                .SetItemCol("itemID")
                .SetCheckpointInterval(10)
                .SetRatingCol("rating")
                .SetAlpha(1.0)
                .SetTimeDecayCoeff(30)
                .SetBlockSize(4096)
                .SetStartTimeFormat("EEE MMM dd HH:mm:ss Z yyyy")
                .SetRegParam(0.1);

            var sarModel = new SARModel()
                // .SetUserDataFrame(userDataFrameDF)
                .SetNumItemBlocks(10)
                // .SetItemDataFrame(itemDataFrameDF)
                .SetImplicitPrefs(false)
                .SetRank(10)
                .SetUserCol("customerID")
                .SetFinalStorageLevel("MEMORY_AND_DISK")
                .SetCheckpointInterval(10)
                .SetNumUserBlocks(10)
                .SetMaxIter(10)
                .SetTimeCol("time")
                .SetRatingCol("rating")
                .SetStartTimeFormat("EEE MMM dd HH:mm:ss Z yyyy")
                .SetItemCol("itemID")
                .SetActivityTimeFormat("yyyy/MM/dd'T'h:mm:ss")
                .SetIntermediateStorageLevel("MEMORY_AND_DISK")
                .SetNonnegative(false)
                .SetBlockSize(4096)
                .SetSupportThreshold(4)
                .SetAlpha(1.0)
                .SetColdStartStrategy("nan")
                .SetSeed(-809975865)
                .SetTimeDecayCoeff(30)
                .SetPredictionCol("prediction")
                .SetRegParam(0.1)
                .SetSimilarityFunction("jaccard");
        }

        [Fact]
        public void TestDictionary()
        {
            var model = new JSONInputParser()
                .SetInputCol("data")
                .SetHeaders(new Dictionary<string, string> { })
                .SetMethod("POST")
                .SetOutputCol("out")
                .SetUrl("http://localhost:55095/foo");

            var returnHeaders = model.GetHeaders();
        }

        [Fact]
        public void TestPipeline()
        {
            var stages = new JavaPipelineStage[] {
                new DictionaryExamples(),
                new TextSentiment()
            };
            var pipeline = new Pipeline().SetStages(stages);
            var returnStages = pipeline.GetStages();
        }

        [Fact]
        public void TestCogServiceSettersAndGetters()
        {
            var bucketizer = new Bucketizer()
                .SetSplitsArray(new double[][] {
                    new []{1.0, 2.0, 3.0}
                });

            var tt = new TextAndTranslation[] {
                    new TextAndTranslation("1", "2"),
                    new TextAndTranslation("1", "2")
                };
            var dictionaryExamples = new DictionaryExamples()
                .SetTextAndTranslation(tt);
            var returnTextAndTranslation = dictionaryExamples.GetTextAndTranslation();

            dictionaryExamples.SetTextAndTranslation(
                new TextAndTranslation[] {
                    new TextAndTranslation(text:"1",translation:"2")
                }
            );

            var textAndTranslation = dictionaryExamples.GetTextAndTranslation();

            var detectLastAnomaly = new DetectLastAnomaly()
                .SetSeries(new TimeSeriesPoint[] {
                    new TimeSeriesPoint("1", 1.0),
                    new TimeSeriesPoint("2", 2.0)
                });

            var model = new TextAnalyze()
                .SetOutputCol("response")
                .SetTimeout(60.0)
                .SetMaxPollingRetries(1000)
                .SetErrorCol("error")
                .SetTextCol("text")
                .SetEntityRecognitionPiiTasks(new TextAnalyzeTask[]{new TextAnalyzeTask(new Dictionary<string, string>{{"model-version","latest"}})})
                .SetPollingDelay(300)
                .SetLanguageCol("language")
                .SetConcurrency(1)
                .SetEntityLinkingTasks(new TextAnalyzeTask[]{new TextAnalyzeTask(new Dictionary<string, string>{{"model-version","latest"}})})
                .SetInitialPollingDelay(300)
                .SetEntityRecognitionTasks(new TextAnalyzeTask[]{new TextAnalyzeTask(new Dictionary<string, string>{{"model-version","latest"}})})
                .SetBackoffs(new int[]
                    {100,500,1000})
                .SetSuppressMaxRetriesExceededException(false)
                .SetKeyPhraseExtractionTasks(new TextAnalyzeTask[]{new TextAnalyzeTask(new Dictionary<string, string>{{"model-version","latest"}})})
                .SetUrl("https://eastus.api.cognitive.microsoft.com//text/analytics/v3.1/analyze")
                .SetSubscriptionKey("df74b0018d394ca0ab2173f3623ca7a1")
                .SetSentimentAnalysisTasks(new TextAnalyzeTask[]{new TextAnalyzeTask(new Dictionary<string, string>{{"model-version","latest"}})});
            
            var entity = model.GetEntityLinkingTasks();

            // // TODO: Fix DiagnosticInfo
            // var modelState = new ModelState(new int[]{1}, new double[]{0.9}, new double[]{0.9}, new double[]{1.0});
            // var variableStates = new DMAVariableState[]{
            //         new DMAVariableState("v1", 0, 1, "startTime", "endTime", null)};

            // var model2 = new Synapse.ML.Cognitive.FitMultivariateAnomaly()
            //     .SetStartTime("2021-01-01T00:00:00Z")
            //     .SetTimestampCol("timestamp")
            //     .SetSuppressMaxRetriesExceededException(false)
            //     .SetUrl("https://westus2.api.cognitive.microsoft.com/anomalydetector/v1.1-preview/multivariate/models")
            //     .SetPollingDelay(300)
            //     .SetIntermediateSaveDir("intermediateData")
            //     .SetEndTime("2021-01-02T12:00:00Z")
            //     .SetInputCols(new string[]
            //         {"feature0","feature1","feature2"})
            //     .SetSlidingWindow(200)
            //     .SetErrorCol("FitMultivariateAnomaly_39cbdb46a714_error")
            //     .SetInitialPollingDelay(300)
            //     .SetMaxPollingRetries(1000)
            //     .SetContainerName("madtest")
            //     .SetSubscriptionKey("4bda24390e4341f89623b7706e7c550f")
            //     .SetOutputCol("result")
            //     .SetBackoffs(new int[]
            //         {100,500,1000});
            //     // .SetDiagnosticsInfo(new DiagnosticsInfo(modelState, variableStates));
            
            // var diagnosticsInfo = model2.GetDiagnosticsInfo();
        }

        [Fact]
        public void TestDotnetUtils()
        {
             // Validated EstimatorParam
            var rankingAdapter = new RankingAdapter()
                .SetK(3)
                .SetRecommender(new RecommendationIndexer().SetUserOutputCol("outputCol"));
            var recommender = rankingAdapter.GetRecommender();
            Console.WriteLine(recommender.GetType());
            // Convert to the recommender.GetType explicitly
            var recommender2 = recommender as RecommendationIndexer;
            Console.WriteLine(recommender2.GetUserOutputCol());
        }

        [Fact]
        public void TestHyperparamBuilder()
        {
            // Validating ParamSpaceParam
            // var hyperParams = new (Param, Dist<Type>)[2];
            var hyperParams = new HyperparamBuilder()
                .AddHyperparam(new Param("null", "maxBins", "maxBins"), new IntRangeHyperParam(16, 32))
                .AddHyperparam(new Param("null", "maxDepth", "maxDepth"), new IntRangeHyperParam(2, 5))
                .AddHyperparam(new Param("null", "minInfoGain", "minInfoGain"), new DoubleRangeHyperParam(0.0, 0.5))
                .Build();
            // TODO: dotnet spark doesn't support tuple in invoke yet!!!
            // var randomeSpace = new RandomSpace(hyperParams);
            // tuneHyperparameters.SetParamSpace(randomeSpace);
        }

        [Fact]
        public void TestArrayMapParam()
        {
            // Validated ArrayMapParam
            var imageStages = new Dictionary<string, object>[] {
                new Dictionary<string, object> {{"1", "2"}, {"2", 3}},
                new Dictionary<string, object> {{"a", true}, {"b", 5}}
            };
            var imageTransformer = new ImageTransformer()
                .SetStages(imageStages);
            var returnImageStages = imageTransformer.GetStages();
        }

        [Fact]
        public void TestAll()
        {

            // Validated PipelineStageParam
            var wordDF = _spark.CreateDataFrame(
                new List<GenericRow>
                {
                    new GenericRow(new object[] {0, "This is a test", "this is one too"}),
                    new GenericRow(new object[] {1, "could be a test", "bar"})
                },
                new StructType(new List<StructField>
                {
                    new StructField("label", new IntegerType()),
                    new StructField("words1", new StringType()),
                    new StructField("words2", new StringType())
                })
            );
            var stage1 = new Tokenizer();
            var multiColumnAdapter = new MultiColumnAdapter()
                .SetBaseStage(stage1)
                .SetInputCols(new string[] { "words1", "words2" })
                .SetOutputCols(new string[] { "output1", "output2" });
            var tokenizedDF = multiColumnAdapter.Fit(wordDF).Transform(wordDF);
            // var baseStage = multiColumnAdapter.GetBaseStage();

            // Validated UntypedArrayParam
            var cleanMissingDataModel = new CleanMissingDataModel()
                .SetFillValues(new object[] { 1, "2", true });
            var returnFillValues = cleanMissingDataModel.GetFillValues();

            // Validated EstimatorArrayParam
            var estimators = new IEstimator<object>[] {
                multiColumnAdapter,
                multiColumnAdapter
            };
            var tuneHyperparameters = new TuneHyperparameters()
                .SetModels(estimators);
            var resturnEstimators = tuneHyperparameters.GetModels();
            foreach (var estimator in resturnEstimators)
            {
                Console.WriteLine(estimator.GetType());
                // Convert to the recommender.GetType explicitly
                var estimator2 = estimator as MultiColumnAdapter;
            }

            // Validated ModelParam & Validated TypedDoubleArrayParam
            var rankingTrainValidationSplitModel = new RankingTrainValidationSplitModel()
                .SetBestModel(new RankingTrainValidationSplitModel().SetValidationMetrics(new double[] { 1.0 }));
            var bestModel = rankingTrainValidationSplitModel.GetBestModel();
            Console.WriteLine(bestModel.GetType());
            // Convert type explicitly
            var bestModel2 = bestModel as RankingTrainValidationSplitModel;
            foreach (double x in bestModel2.GetValidationMetrics())
            {
                Console.WriteLine(x);
            }


            // Validated TransformerParam & DataTypeParam
            var jSONOutputParser = new JSONOutputParser().SetDataType(
                new StructType(new List<StructField> { new StructField("blah", new StringType()) })
            );
            var returnDataType = jSONOutputParser.GetDataType();
            var simpleHTTPTransformer = new SimpleHTTPTransformer()
                .SetInputCol("data")
                .SetOutputParser(jSONOutputParser)
                .SetOutputCol("results");
            var outputParser = simpleHTTPTransformer.GetOutputParser();

            // Validated EstimatorParam
            var rankingAdapter = new RankingAdapter()
                .SetK(3)
                .SetRecommender(new RecommendationIndexer().SetUserOutputCol("outputCol"));
            var recommender = rankingAdapter.GetRecommender();
            Console.WriteLine(recommender.GetType());
            // Convert to the recommender.GetType explicitly
            var recommender2 = recommender as RecommendationIndexer;
            Console.WriteLine(recommender2.GetUserOutputCol());

            // Validated TypedIntArrayParam
            var multiNGram = new MultiNGram().SetLengths(new int[] { 1, 2, 3 });
            foreach (int x in multiNGram.GetLengths())
            {
                Console.WriteLine(x);
            }


            // Validated DataFrameParam
            DataFrame df = _spark.CreateDataFrame(
               new List<GenericRow>
               {
                    new GenericRow(new object[] { 2.0D, true, "1", "foo" }),
                    new GenericRow(new object[] { 3.0D, false, "2", "bar" })
               },
               new StructType(new List<StructField>
               {
                    new StructField("real", new DoubleType()),
                    new StructField("bool", new BooleanType()),
                    new StructField("stringNum", new StringType()),
                    new StructField("string", new StringType())
               }));

            var vectorLIME = new VectorLIME()
                .SetModel(new RankingTrainValidationSplitModel())
                .SetBackgroundData(df);
            var returnBackgroundData = vectorLIME.GetBackgroundData();


            // Validated Fit/Transform DataFrame
            var recommendationIndexer = new RecommendationIndexer()
                .SetUserInputCol("customerIDOrg")
                .SetUserOutputCol("customerID")
                .SetItemInputCol("itemIDOrg")
                .SetItemOutputCol("itemID")
                .SetRatingCol("rating");

            DataFrame ratings = _spark.CreateDataFrame(
                new List<GenericRow>
                {
                    new GenericRow(new object[] {"11", "Movie 01", 2}),
                    new GenericRow(new object[] {"11", "Movie 03", 1})
                },
                new StructType(new List<StructField>
                {
                    new StructField("customerIDOrg", new StringType()),
                    new StructField("itemIDOrg", new StringType()),
                    new StructField("rating", new IntegerType())
                })
            );

            var transformedDf = recommendationIndexer.Fit(ratings).Transform(ratings);

            // Validated EvaluatorParam
            var evaluator = new RankingEvaluator()
                .SetK(3)
                .SetNItems(10);

            var als = new ALS()
                .SetNumUserBlocks(1)
                .SetNumItemBlocks(1)
                .SetUserCol(recommendationIndexer.GetUserOutputCol())
                .SetItemCol(recommendationIndexer.GetItemOutputCol())
                .SetRatingCol("rating")
                .SetSeed(0);

            var rankingTrainValidationSplit = new RankingTrainValidationSplit()
                .SetEstimator(als)
                .SetEvaluator(evaluator)
                .SetTrainRatio(0.8)
                .SetUserCol(recommendationIndexer.GetUserOutputCol())
                .SetItemCol(recommendationIndexer.GetItemOutputCol())
                .SetRatingCol("rating");

            var returnEvaluator = rankingTrainValidationSplit.GetEvaluator();


            // Validated TransformerArrayParam
            var models = new JavaTransformer[] {
                new TextSentiment(),
                new TextSentiment()
            };
            var findBestModel = new FindBestModel()
                .SetModels(models);
            var returnModels = findBestModel.GetModels();
            foreach (var model in returnModels)
            {
                Console.WriteLine(model.GetType());
                // Convert to the recommender.GetType explicitly
                var model2 = model as TextSentiment;
                model2.GetErrorCol();
            }

            // Validated ArrayParamMapParam
            Param regParam = new Param("test", "regParam", "regularization parameter");
            var paramGrid = new ParamMap[] {
                new ParamMap().Put(regParam, new double[] {1.0})
            };
            rankingTrainValidationSplit.SetEstimatorParamMaps(paramGrid);
            var tvModel = rankingTrainValidationSplit.Fit(transformedDf);
            bestModel = tvModel.GetBestModel();
            var alsBestModel = bestModel as ALSModel;
            // alsBestModel.Write().Overwrite().Save("D:\\repos\\SynapseML\\dotnetClasses\\haha\\");
            var returnParamMaps = rankingTrainValidationSplit.GetEstimatorParamMaps();



            // LightGBMBoosterParam
            var lightGBMClassificationModel = new LightGBMClassificationModel()
                .SetLightGBMBooster(new LightGBMBooster("test"));

            Assert.Equal("1", "1");

        }

        [Fact]
        public void TestLightGBMParams()
        {

            var featureColumns = new string[] {
                "Number of times pregnant","Plasma glucose concentration a 2 hours in an oral glucose tolerance test",
                "Diastolic blood pressure (mm Hg)","Triceps skin fold thickness (mm)","2-Hour serum insulin (mu U/ml)",
                "Body mass index (weight in kg/(height in m)^2)","Diabetes pedigree function","Age (years)"
            };

            var df = _spark.CreateDataFrame(
                new List<GenericRow>
                {
                    new GenericRow(new object[] {0,131,66,40,0,34.3,0.196,22,1}),
                    new GenericRow(new object[] {7,194,68,28,0,35.9,0.745,41,1}),
                    new GenericRow(new object[] {3,139,54,0,0,25.6,0.402,22,1}),
                    new GenericRow(new object[] {6,134,70,23,130,35.4,0.542,29,1}),
                    new GenericRow(new object[] {9,124,70,33,402,35.4,0.282,34,0}),
                    new GenericRow(new object[] {0,93,100,39,72,43.4,1.021,35,0}),
                    new GenericRow(new object[] {4,110,76,20,100,28.4,0.118,27,0}),
                    new GenericRow(new object[] {2,127,58,24,275,27.7,1.6,25,0}),
                    new GenericRow(new object[] {0,104,64,37,64,33.6,0.51,22,1}),
                    new GenericRow(new object[] {2,120,54,0,0,26.8,0.455,27,0}),
                    new GenericRow(new object[] {7,178,84,0,0,39.9,0.331,41,1}),
                    new GenericRow(new object[] {2,88,58,26,16,28.4,0.766,22,0}),
                    new GenericRow(new object[] {1,91,64,24,0,29.2,0.192,21,0}),
                    new GenericRow(new object[] {10,101,76,48,180,32.9,0.171,63,0}),
                    new GenericRow(new object[] {5,73,60,0,0,26.8,0.268,27,0}),
                    new GenericRow(new object[] {3,158,70,30,328,35.5,0.344,35,1}),
                    new GenericRow(new object[] {2,105,75,0,0,23.3,0.56,53,0}),
                    new GenericRow(new object[] {12,84,72,31,0,29.7,0.297,46,1}),
                    new GenericRow(new object[] {9,119,80,35,0,29.0,0.263,29,1}),
                    new GenericRow(new object[] {6,93,50,30,64,28.7,0.356,23,0}),
                    new GenericRow(new object[] {1,126,60,0,0,30.1,0.349,47,1})
                },
                new StructType(new List<StructField>
                {
                    new StructField("Number of times pregnant", new IntegerType()),
                    new StructField("Plasma glucose concentration a 2 hours in an oral glucose tolerance test", new IntegerType()),
                    new StructField("Diastolic blood pressure (mm Hg)", new IntegerType()),
                    new StructField("Triceps skin fold thickness (mm)", new IntegerType()),
                    new StructField("2-Hour serum insulin (mu U/ml)", new IntegerType()),
                    new StructField("Body mass index (weight in kg/(height in m)^2)", new DoubleType()),
                    new StructField("Diabetes pedigree function", new DoubleType()),
                    new StructField("Age (years)", new IntegerType()),
                    new StructField("labels", new IntegerType())
                })
            ).Repartition(2);

            var featurize = new Featurize()
                .SetOutputCol("features")
                .SetInputCols(featureColumns)
                .SetOneHotEncodeCategoricals(true)
                .SetNumFeatures(4096);
            
            var dfTrans = featurize.Fit(df).Transform(df);

            var lightGBMClassifier = new LightGBMClassifier()
                .SetFeaturesCol("features")
                .SetRawPredictionCol("rawPrediction")
                .SetDefaultListenPort(12402)
                .SetNumLeaves(10)
                .SetNumIterations(100)
                .SetObjective("binary")
                .SetLabelCol("labels")
                .SetLeafPredictionCol("leafPrediction")
                .SetFeaturesShapCol("featuresShap");
            
            var lightGBMClassificationModel = lightGBMClassifier.Fit(dfTrans);

            lightGBMClassificationModel.Transform(dfTrans).Show();

            var lightGBMBooster = new LightGBMBooster("test");
            lightGBMClassificationModel.SetLightGBMBooster(lightGBMBooster);
            var returnBooster = lightGBMClassificationModel.GetLightGBMBooster();

        }


    }

}

