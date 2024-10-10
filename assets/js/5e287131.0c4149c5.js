"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[35668],{3905:(e,t,a)=>{a.d(t,{Zo:()=>p,kt:()=>g});var n=a(67294);function i(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function r(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function l(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?r(Object(a),!0).forEach((function(t){i(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):r(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function s(e,t){if(null==e)return{};var a,n,i=function(e,t){if(null==e)return{};var a,n,i={},r=Object.keys(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||(i[a]=e[a]);return i}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(i[a]=e[a])}return i}var o=n.createContext({}),d=function(e){var t=n.useContext(o),a=t;return e&&(a="function"==typeof e?e(t):l(l({},t),e)),a},p=function(e){var t=d(e.components);return n.createElement(o.Provider,{value:t},e.children)},m={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},c=n.forwardRef((function(e,t){var a=e.components,i=e.mdxType,r=e.originalType,o=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),c=d(a),g=i,u=c["".concat(o,".").concat(g)]||c[g]||m[g]||r;return a?n.createElement(u,l(l({ref:t},p),{},{components:a})):n.createElement(u,l({ref:t},p))}));function g(e,t){var a=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var r=a.length,l=new Array(r);l[0]=c;var s={};for(var o in t)hasOwnProperty.call(t,o)&&(s[o]=t[o]);s.originalType=e,s.mdxType="string"==typeof e?e:i,l[1]=s;for(var d=2;d<r;d++)l[d]=a[d];return n.createElement.apply(null,l)}return n.createElement.apply(null,a)}c.displayName="MDXCreateElement"},94098:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>o,contentTitle:()=>l,default:()=>m,frontMatter:()=>r,metadata:()=>s,toc:()=>d});var n=a(83117),i=(a(67294),a(3905));const r={title:"Quickstart - Classification, Ranking, and Regression",hide_title:!0,status:"stable"},l="LightGBM",s={unversionedId:"Explore Algorithms/LightGBM/Quickstart - Classification, Ranking, and Regression",id:"version-1.0.5/Explore Algorithms/LightGBM/Quickstart - Classification, Ranking, and Regression",title:"Quickstart - Classification, Ranking, and Regression",description:"What is LightGBM",source:"@site/versioned_docs/version-1.0.5/Explore Algorithms/LightGBM/Quickstart - Classification, Ranking, and Regression.md",sourceDirName:"Explore Algorithms/LightGBM",slug:"/Explore Algorithms/LightGBM/Quickstart - Classification, Ranking, and Regression",permalink:"/SynapseML/docs/1.0.5/Explore Algorithms/LightGBM/Quickstart - Classification, Ranking, and Regression",draft:!1,tags:[],version:"1.0.5",frontMatter:{title:"Quickstart - Classification, Ranking, and Regression",hide_title:!0,status:"stable"},sidebar:"docs",previous:{title:"Overview",permalink:"/SynapseML/docs/1.0.5/Explore Algorithms/LightGBM/Overview"},next:{title:"Overview",permalink:"/SynapseML/docs/1.0.5/Explore Algorithms/AI Services/Overview"}},o={},d=[{value:"What is LightGBM",id:"what-is-lightgbm",level:2},{value:"Advantages of LightGBM",id:"advantages-of-lightgbm",level:3},{value:"LightGBM Usage",id:"lightgbm-usage",level:3},{value:"Use <code>LightGBMClassifier</code> to train a classification model",id:"use-lightgbmclassifier-to-train-a-classification-model",level:2},{value:"Read dataset",id:"read-dataset",level:3},{value:"Split the dataset into train and test sets",id:"split-the-dataset-into-train-and-test-sets",level:3},{value:"Add a featurizer to convert features into vectors",id:"add-a-featurizer-to-convert-features-into-vectors",level:3},{value:"Check if the data is unbalanced",id:"check-if-the-data-is-unbalanced",level:3},{value:"Model Training",id:"model-training",level:3},{value:"Visualize feature importance",id:"visualize-feature-importance",level:3},{value:"Generate predictions with the model",id:"generate-predictions-with-the-model",level:3},{value:"Use <code>LightGBMRegressor</code> to train a quantile regression model",id:"use-lightgbmregressor-to-train-a-quantile-regression-model",level:2},{value:"Read dataset",id:"read-dataset-1",level:3},{value:"Split dataset into train and test sets",id:"split-dataset-into-train-and-test-sets",level:3},{value:"Train the model using <code>LightGBMRegressor</code>",id:"train-the-model-using-lightgbmregressor",level:3},{value:"Generate predictions with the model",id:"generate-predictions-with-the-model-1",level:3},{value:"Use <code>LightGBMRanker</code> to train a ranking model",id:"use-lightgbmranker-to-train-a-ranking-model",level:2},{value:"Read the dataset",id:"read-the-dataset",level:3},{value:"Train the ranking model using <code>LightGBMRanker</code>.",id:"train-the-ranking-model-using-lightgbmranker",level:3},{value:"Generate predictions with the model",id:"generate-predictions-with-the-model-2",level:3}],p={toc:d};function m(e){let{components:t,...a}=e;return(0,i.kt)("wrapper",(0,n.Z)({},p,a,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"lightgbm"},"LightGBM"),(0,i.kt)("h2",{id:"what-is-lightgbm"},"What is LightGBM"),(0,i.kt)("p",null,(0,i.kt)("a",{parentName:"p",href:"https://github.com/Microsoft/LightGBM"},"LightGBM")," is an open-source,\ndistributed, high-performance gradient boosting (GBDT, GBRT, GBM, or\nMART) framework. This framework specializes in creating high-quality and\nGPU-enabled decision tree algorithms for ranking, classification, and\nmany other machine learning tasks. LightGBM is part of Microsoft's\n",(0,i.kt)("a",{parentName:"p",href:"https://github.com/microsoft/dmtk"},"DMTK")," project."),(0,i.kt)("h3",{id:"advantages-of-lightgbm"},"Advantages of LightGBM"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Composability"),": LightGBM models can be incorporated into existing\nSparkML pipelines and used for batch, streaming, and serving\nworkloads."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Performance"),": LightGBM on Spark is 10-30% faster than SparkML on\nthe ",(0,i.kt)("a",{parentName:"li",href:"https://archive.ics.uci.edu/dataset/280/higgs"},"Higgs dataset")," and achieves a 15% increase in AUC.  ",(0,i.kt)("a",{parentName:"li",href:"https://github.com/Microsoft/LightGBM/blob/master/docs/Experiments.rst#parallel-experiment"},"Parallel\nexperiments"),"\nhave verified that LightGBM can achieve a linear speed-up by using\nmultiple machines for training in specific settings."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Functionality"),": LightGBM offers a wide array of ",(0,i.kt)("a",{parentName:"li",href:"https://github.com/Microsoft/LightGBM/blob/master/docs/Parameters.rst"},"tunable\nparameters"),",\nthat one can use to customize their decision tree system. LightGBM on\nSpark also supports new types of problems such as quantile regression."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"Cross platform"),": LightGBM on Spark is available on Spark, PySpark, and SparklyR.")),(0,i.kt)("h3",{id:"lightgbm-usage"},"LightGBM Usage"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"LightGBMClassifier"),": used for building classification models. For example, to predict whether a company bankrupts or not, we could build a binary classification model with ",(0,i.kt)("inlineCode",{parentName:"li"},"LightGBMClassifier"),"."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"LightGBMRegressor"),": used for building regression models. For example, to predict housing price, we could build a regression model with ",(0,i.kt)("inlineCode",{parentName:"li"},"LightGBMRegressor"),"."),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"LightGBMRanker"),": used for building ranking models. For example, to predict the relevance of website search results, we could build a ranking model with ",(0,i.kt)("inlineCode",{parentName:"li"},"LightGBMRanker"),".")),(0,i.kt)("h2",{id:"use-lightgbmclassifier-to-train-a-classification-model"},"Use ",(0,i.kt)("inlineCode",{parentName:"h2"},"LightGBMClassifier")," to train a classification model"),(0,i.kt)("p",null,"In this example, we use LightGBM to build a classification model in order to predict bankruptcy."),(0,i.kt)("h3",{id:"read-dataset"},"Read dataset"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},"from synapse.ml.core.platform import *\n")),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'df = (\n    spark.read.format("csv")\n    .option("header", True)\n    .option("inferSchema", True)\n    .load(\n        "wasbs://publicwasb@mmlspark.blob.core.windows.net/company_bankruptcy_prediction_data.csv"\n    )\n)\n# print dataset size\nprint("records read: " + str(df.count()))\nprint("Schema: ")\ndf.printSchema()\n')),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},"display(df)\n")),(0,i.kt)("h3",{id:"split-the-dataset-into-train-and-test-sets"},"Split the dataset into train and test sets"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},"train, test = df.randomSplit([0.85, 0.15], seed=1)\n")),(0,i.kt)("h3",{id:"add-a-featurizer-to-convert-features-into-vectors"},"Add a featurizer to convert features into vectors"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'from pyspark.ml.feature import VectorAssembler\n\nfeature_cols = df.columns[1:]\nfeaturizer = VectorAssembler(inputCols=feature_cols, outputCol="features")\ntrain_data = featurizer.transform(train)["Bankrupt?", "features"]\ntest_data = featurizer.transform(test)["Bankrupt?", "features"]\n')),(0,i.kt)("h3",{id:"check-if-the-data-is-unbalanced"},"Check if the data is unbalanced"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'display(train_data.groupBy("Bankrupt?").count())\n')),(0,i.kt)("h3",{id:"model-training"},"Model Training"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.lightgbm import LightGBMClassifier\n\nmodel = LightGBMClassifier(\n    objective="binary", featuresCol="features", labelCol="Bankrupt?", isUnbalance=True\n)\n')),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},"model = model.fit(train_data)\n")),(0,i.kt)("p",null,'"saveNativeModel" allows you to extract the underlying lightGBM model for fast deployment after you train on Spark.'),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.lightgbm import LightGBMClassificationModel\n\nif running_on_synapse():\n    model.saveNativeModel("/models/lgbmclassifier.model")\n    model = LightGBMClassificationModel.loadNativeModelFromFile(\n        "/models/lgbmclassifier.model"\n    )\nif running_on_synapse_internal():\n    model.saveNativeModel("Files/models/lgbmclassifier.model")\n    model = LightGBMClassificationModel.loadNativeModelFromFile(\n        "Files/models/lgbmclassifier.model"\n    )\nelse:\n    model.saveNativeModel("/tmp/lgbmclassifier.model")\n    model = LightGBMClassificationModel.loadNativeModelFromFile(\n        "/tmp/lgbmclassifier.model"\n    )\n')),(0,i.kt)("h3",{id:"visualize-feature-importance"},"Visualize feature importance"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'import pandas as pd\nimport matplotlib.pyplot as plt\n\nfeature_importances = model.getFeatureImportances()\nfi = pd.Series(feature_importances, index=feature_cols)\nfi = fi.sort_values(ascending=True)\nf_index = fi.index\nf_values = fi.values\n\n# print feature importances\nprint("f_index:", f_index)\nprint("f_values:", f_values)\n\n# plot\nx_index = list(range(len(fi)))\nx_index = [x / len(fi) for x in x_index]\nplt.rcParams["figure.figsize"] = (20, 20)\nplt.barh(\n    x_index, f_values, height=0.028, align="center", color="tan", tick_label=f_index\n)\nplt.xlabel("importances")\nplt.ylabel("features")\nplt.show()\n')),(0,i.kt)("h3",{id:"generate-predictions-with-the-model"},"Generate predictions with the model"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},"predictions = model.transform(test_data)\npredictions.limit(10).toPandas()\n")),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.train import ComputeModelStatistics\n\nmetrics = ComputeModelStatistics(\n    evaluationMetric="classification",\n    labelCol="Bankrupt?",\n    scoredLabelsCol="prediction",\n).transform(predictions)\ndisplay(metrics)\n')),(0,i.kt)("h2",{id:"use-lightgbmregressor-to-train-a-quantile-regression-model"},"Use ",(0,i.kt)("inlineCode",{parentName:"h2"},"LightGBMRegressor")," to train a quantile regression model"),(0,i.kt)("p",null,"In this example, we show how to use LightGBM to build a regression model."),(0,i.kt)("h3",{id:"read-dataset-1"},"Read dataset"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'triazines = spark.read.format("libsvm").load(\n    "wasbs://publicwasb@mmlspark.blob.core.windows.net/triazines.scale.svmlight"\n)\n')),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'# print some basic info\nprint("records read: " + str(triazines.count()))\nprint("Schema: ")\ntriazines.printSchema()\ndisplay(triazines.limit(10))\n')),(0,i.kt)("h3",{id:"split-dataset-into-train-and-test-sets"},"Split dataset into train and test sets"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},"train, test = triazines.randomSplit([0.85, 0.15], seed=1)\n")),(0,i.kt)("h3",{id:"train-the-model-using-lightgbmregressor"},"Train the model using ",(0,i.kt)("inlineCode",{parentName:"h3"},"LightGBMRegressor")),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.lightgbm import LightGBMRegressor\n\nmodel = LightGBMRegressor(\n    objective="quantile", alpha=0.2, learningRate=0.3, numLeaves=31\n).fit(train)\n')),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},"print(model.getFeatureImportances())\n")),(0,i.kt)("h3",{id:"generate-predictions-with-the-model-1"},"Generate predictions with the model"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},"scoredData = model.transform(test)\ndisplay(scoredData)\n")),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.train import ComputeModelStatistics\n\nmetrics = ComputeModelStatistics(\n    evaluationMetric="regression", labelCol="label", scoresCol="prediction"\n).transform(scoredData)\ndisplay(metrics)\n')),(0,i.kt)("h2",{id:"use-lightgbmranker-to-train-a-ranking-model"},"Use ",(0,i.kt)("inlineCode",{parentName:"h2"},"LightGBMRanker")," to train a ranking model"),(0,i.kt)("h3",{id:"read-the-dataset"},"Read the dataset"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'df = spark.read.format("parquet").load(\n    "wasbs://publicwasb@mmlspark.blob.core.windows.net/lightGBMRanker_train.parquet"\n)\n# print some basic info\nprint("records read: " + str(df.count()))\nprint("Schema: ")\ndf.printSchema()\ndisplay(df.limit(10))\n')),(0,i.kt)("h3",{id:"train-the-ranking-model-using-lightgbmranker"},"Train the ranking model using ",(0,i.kt)("inlineCode",{parentName:"h3"},"LightGBMRanker"),"."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.lightgbm import LightGBMRanker\n\nfeatures_col = "features"\nquery_col = "query"\nlabel_col = "labels"\nlgbm_ranker = LightGBMRanker(\n    labelCol=label_col,\n    featuresCol=features_col,\n    groupCol=query_col,\n    predictionCol="preds",\n    leafPredictionCol="leafPreds",\n    featuresShapCol="importances",\n    repartitionByGroupingColumn=True,\n    numLeaves=32,\n    numIterations=200,\n    evalAt=[1, 3, 5],\n    metric="ndcg",\n)\n')),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},"lgbm_ranker_model = lgbm_ranker.fit(df)\n")),(0,i.kt)("h3",{id:"generate-predictions-with-the-model-2"},"Generate predictions with the model"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'dt = spark.read.format("parquet").load(\n    "wasbs://publicwasb@mmlspark.blob.core.windows.net/lightGBMRanker_test.parquet"\n)\npredictions = lgbm_ranker_model.transform(dt)\npredictions.limit(10).toPandas()\n')))}m.isMDXComponent=!0}}]);