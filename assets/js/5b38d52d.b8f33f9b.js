"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[33310],{3905:(e,t,a)=>{a.d(t,{Zo:()=>m,kt:()=>d});var n=a(67294);function o(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function r(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function l(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?r(Object(a),!0).forEach((function(t){o(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):r(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function i(e,t){if(null==e)return{};var a,n,o=function(e,t){if(null==e)return{};var a,n,o={},r=Object.keys(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||(o[a]=e[a]);return o}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(o[a]=e[a])}return o}var s=n.createContext({}),p=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):l(l({},t),e)),a},m=function(e){var t=p(e.components);return n.createElement(s.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},u=n.forwardRef((function(e,t){var a=e.components,o=e.mdxType,r=e.originalType,s=e.parentName,m=i(e,["components","mdxType","originalType","parentName"]),u=p(a),d=o,h=u["".concat(s,".").concat(d)]||u[d]||c[d]||r;return a?n.createElement(h,l(l({ref:t},m),{},{components:a})):n.createElement(h,l({ref:t},m))}));function d(e,t){var a=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var r=a.length,l=new Array(r);l[0]=u;var i={};for(var s in t)hasOwnProperty.call(t,s)&&(i[s]=t[s]);i.originalType=e,i.mdxType="string"==typeof e?e:o,l[1]=i;for(var p=2;p<r;p++)l[p]=a[p];return n.createElement.apply(null,l)}return n.createElement.apply(null,a)}u.displayName="MDXCreateElement"},60094:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>s,contentTitle:()=>l,default:()=>c,frontMatter:()=>r,metadata:()=>i,toc:()=>p});var n=a(83117),o=(a(67294),a(3905));const r={title:"Quickstart - Isolation Forests",hide_title:!0,status:"stable"},l="Multivariate Anomaly Detection with Isolation Forest",i={unversionedId:"Explore Algorithms/Anomaly Detection/Quickstart - Isolation Forests",id:"version-1.0.6/Explore Algorithms/Anomaly Detection/Quickstart - Isolation Forests",title:"Quickstart - Isolation Forests",description:"This article shows how you can use SynapseML on Apache Spark for multivariate anomaly detection. Multivariate anomaly detection allows for the detection of anomalies among many variables or time series, taking into account all the inter-correlations and dependencies between the different variables. In this scenario, we use SynapseML to train an Isolation Forest model for multivariate anomaly detection, and we then use to the trained model to infer multivariate anomalies within a dataset containing synthetic measurements from three IoT sensors.",source:"@site/versioned_docs/version-1.0.6/Explore Algorithms/Anomaly Detection/Quickstart - Isolation Forests.md",sourceDirName:"Explore Algorithms/Anomaly Detection",slug:"/Explore Algorithms/Anomaly Detection/Quickstart - Isolation Forests",permalink:"/SynapseML/docs/1.0.6/Explore Algorithms/Anomaly Detection/Quickstart - Isolation Forests",draft:!1,tags:[],version:"1.0.6",frontMatter:{title:"Quickstart - Isolation Forests",hide_title:!0,status:"stable"},sidebar:"docs",previous:{title:"Quickstart - Vowpal Wabbit and LightGBM",permalink:"/SynapseML/docs/1.0.6/Explore Algorithms/Regression/Quickstart - Vowpal Wabbit and LightGBM"},next:{title:"HyperOpt",permalink:"/SynapseML/docs/1.0.6/Explore Algorithms/Hyperparameter Tuning/HyperOpt"}},s={},p=[{value:"Prerequisites",id:"prerequisites",level:2},{value:"Library imports",id:"library-imports",level:2},{value:"Input data",id:"input-data",level:2},{value:"Read data",id:"read-data",level:2},{value:"Training data preparation",id:"training-data-preparation",level:2},{value:"Test data preparation",id:"test-data-preparation",level:2},{value:"Train Isolation Forest model",id:"train-isolation-forest-model",level:2},{value:"Perform inferencing",id:"perform-inferencing",level:2},{value:"ML interpretability",id:"ml-interpretability",level:2},{value:"Visualize results",id:"visualize-results",level:2}],m={toc:p};function c(e){let{components:t,...a}=e;return(0,o.kt)("wrapper",(0,n.Z)({},m,a,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"multivariate-anomaly-detection-with-isolation-forest"},"Multivariate Anomaly Detection with Isolation Forest"),(0,o.kt)("p",null,"This article shows how you can use SynapseML on Apache Spark for multivariate anomaly detection. Multivariate anomaly detection allows for the detection of anomalies among many variables or time series, taking into account all the inter-correlations and dependencies between the different variables. In this scenario, we use SynapseML to train an Isolation Forest model for multivariate anomaly detection, and we then use to the trained model to infer multivariate anomalies within a dataset containing synthetic measurements from three IoT sensors."),(0,o.kt)("p",null,"To learn more about the Isolation Forest model please refer to the original paper by ",(0,o.kt)("a",{parentName:"p",href:"https://cs.nju.edu.cn/zhouzh/zhouzh.files/publication/icdm08b.pdf?q=isolation-forest"},"Liu ",(0,o.kt)("em",{parentName:"a"},"et al.")),"."),(0,o.kt)("h2",{id:"prerequisites"},"Prerequisites"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"If running on Synapse, you'll need to ",(0,o.kt)("a",{parentName:"li",href:"/SynapseML/docs/1.0.6/Use%20with%20MLFlow/Overview"},"create an AML workspace and set up linked Service")," and add the following installation cell."),(0,o.kt)("li",{parentName:"ul"},"If running on Fabric, you need to add the following installation cell and attach the notebook to a lakehouse. On the left side of your notebook, select Add to add an existing lakehouse or create a new one.")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'# %%configure -f\n# {\n#   "name": "synapseml",\n#   "conf": {\n#       "spark.jars.packages": "com.microsoft.azure:synapseml_2.12:<THE_SYNAPSEML_VERSION_YOU_WANT>",\n#       "spark.jars.repositories": "https://mmlspark.azureedge.net/maven",\n#       "spark.jars.excludes": "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.12,org.scalactic:scalactic_2.12,org.scalatest:scalatest_2.12,com.fasterxml.jackson.core:jackson-databind",\n#       "spark.yarn.user.classpath.first": "true",\n#       "spark.sql.parquet.enableVectorizedReader": "false"\n#   }\n# }\n')),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"%pip install sqlparse raiwidgets interpret-community mlflow==2.6.0 numpy==1.22.4\n")),(0,o.kt)("h2",{id:"library-imports"},"Library imports"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"import uuid\nimport mlflow\n\nfrom pyspark.sql import functions as F\nfrom pyspark.ml.feature import VectorAssembler\nfrom pyspark.sql.types import *\nfrom pyspark.ml import Pipeline\n\nfrom synapse.ml.isolationforest import *\nfrom synapse.ml.explainers import *\nfrom synapse.ml.core.platform import *\nfrom synapse.ml.isolationforest import *\n")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"# %matplotlib inline\n")),(0,o.kt)("h2",{id:"input-data"},"Input data"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'# Table inputs\ntimestampColumn = "timestamp"  # str: the name of the timestamp column in the table\ninputCols = [\n    "sensor_1",\n    "sensor_2",\n    "sensor_3",\n]  # list(str): the names of the input variables\n\n# Training Start time, and number of days to use for training:\ntrainingStartTime = (\n    "2022-02-24T06:00:00Z"  # datetime: datetime for when to start the training\n)\ntrainingEndTime = (\n    "2022-03-08T23:55:00Z"  # datetime: datetime for when to end the training\n)\ninferenceStartTime = (\n    "2022-03-09T09:30:00Z"  # datetime: datetime for when to start the training\n)\ninferenceEndTime = (\n    "2022-03-20T23:55:00Z"  # datetime: datetime for when to end the training\n)\n\n# Isolation Forest parameters\ncontamination = 0.021\nnum_estimators = 100\nmax_samples = 256\nmax_features = 1.0\n\n# MLFlow experiment\nartifact_path = "isolationforest"\nmodel_name = f"isolation-forest-model"\n\nplatform = current_platform()\nexperiment_name = {\n    "databricks": f"/Shared/isolation_forest_experiment-{str(uuid.uuid1())}/",\n    "synapse": f"isolation_forest_experiment-{str(uuid.uuid1())}",\n    "synapse_internal": f"isolation_forest_experiment-{str(uuid.uuid1())}",  # Fabric\n}.get(platform, f"isolation_forest_experiment")\n')),(0,o.kt)("h2",{id:"read-data"},"Read data"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'df = (\n    spark.read.format("csv")\n    .option("header", "true")\n    .load(\n        "wasbs://publicwasb@mmlspark.blob.core.windows.net/generated_sample_mvad_data.csv"\n    )\n)\n')),(0,o.kt)("p",null,"cast columns to appropriate data types"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'df = (\n    df.orderBy(timestampColumn)\n    .withColumn("timestamp", F.date_format(timestampColumn, "yyyy-MM-dd\'T\'HH:mm:ss\'Z\'"))\n    .withColumn("sensor_1", F.col("sensor_1").cast(DoubleType()))\n    .withColumn("sensor_2", F.col("sensor_2").cast(DoubleType()))\n    .withColumn("sensor_3", F.col("sensor_3").cast(DoubleType()))\n    .drop("_c5")\n)\n\ndisplay(df)\n')),(0,o.kt)("h2",{id:"training-data-preparation"},"Training data preparation"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"# filter to data with timestamps within the training window\ndf_train = df.filter(\n    (F.col(timestampColumn) >= trainingStartTime)\n    & (F.col(timestampColumn) <= trainingEndTime)\n)\ndisplay(df_train.limit(5))\n")),(0,o.kt)("h2",{id:"test-data-preparation"},"Test data preparation"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"# filter to data with timestamps within the inference window\ndf_test = df.filter(\n    (F.col(timestampColumn) >= inferenceStartTime)\n    & (F.col(timestampColumn) <= inferenceEndTime)\n)\ndisplay(df_test.limit(5))\n")),(0,o.kt)("h2",{id:"train-isolation-forest-model"},"Train Isolation Forest model"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'isolationForest = (\n    IsolationForest()\n    .setNumEstimators(num_estimators)\n    .setBootstrap(False)\n    .setMaxSamples(max_samples)\n    .setMaxFeatures(max_features)\n    .setFeaturesCol("features")\n    .setPredictionCol("predictedLabel")\n    .setScoreCol("outlierScore")\n    .setContamination(contamination)\n    .setContaminationError(0.01 * contamination)\n    .setRandomSeed(1)\n)\n')),(0,o.kt)("p",null,"Next, we create an ML pipeline to train the Isolation Forest model. We also demonstrate how to create an MLFlow experiment and register the trained model."),(0,o.kt)("p",null,"Note that MLFlow model registration is strictly only required if accessing the trained model at a later time. For training the model, and performing inferencing in the same notebook, the model object model is sufficient."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'if running_on_synapse():\n    from synapse.ml.core.platform import find_secret\n\n    tracking_url = find_secret(\n        secret_name="aml-mlflow-tracking-url", keyvault="mmlspark-build-keys"\n    )  # check link in prerequisites for more information on mlflow tracking url\n    mlflow.set_tracking_uri(tracking_url)\n')),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'mlflow.set_experiment(experiment_name)\nwith mlflow.start_run() as run:\n    va = VectorAssembler(inputCols=inputCols, outputCol="features")\n    pipeline = Pipeline(stages=[va, isolationForest])\n    model = pipeline.fit(df_train)\n    mlflow.spark.log_model(\n        model, artifact_path=artifact_path, registered_model_name=model_name\n    )\n')),(0,o.kt)("h2",{id:"perform-inferencing"},"Perform inferencing"),(0,o.kt)("p",null,"Load the trained Isolation Forest Model"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'# if running_on_databricks():\n#     model_version = <your_model_version>\n#     model_uri = f"models:/{model_name}/{model_version}"\n# elif running_on_synapse_internal():\n#     model_uri = "runs:/{run_id}/{artifact_path}".format(\n#         run_id=run.info.run_id, artifact_path=artifact_path\n#     )\n# model = mlflow.spark.load_model(model_uri)\n')),(0,o.kt)("p",null,"Perform inferencing"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"df_test_pred = model.transform(df_test)\ndisplay(df_test_pred.limit(5))\n")),(0,o.kt)("h2",{id:"ml-interpretability"},"ML interpretability"),(0,o.kt)("p",null,"In this section, we use ML interpretability tools to help unpack the contribution of each sensor to the detected anomalies at any point in time."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'# Here, we create a TabularSHAP explainer, set the input columns to all the features the model takes, specify the model and the target output column\n# we are trying to explain. In this case, we are trying to explain the "outlierScore" output.\nshap = TabularSHAP(\n    inputCols=inputCols,\n    outputCol="shapValues",\n    model=model,\n    targetCol="outlierScore",\n    backgroundData=F.broadcast(df_test.sample(0.02)),\n)\n')),(0,o.kt)("p",null,"Display the dataframe with ",(0,o.kt)("inlineCode",{parentName:"p"},"shapValues")," column"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"shap_df = shap.transform(df_test_pred)\n")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"# Define UDF\nvec2array = F.udf(lambda vec: vec.toArray().tolist(), ArrayType(FloatType()))\n")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'# Here, we extract the SHAP values, the original features and the outlier score column. Then we convert it to a Pandas DataFrame for visualization.\n# For each observation, the first element in the SHAP values vector is the base value (the mean output of the background dataset),\n# and each of the following elements represents the SHAP values for each feature\nshaps = (\n    shap_df.withColumn("shapValues", vec2array(F.col("shapValues").getItem(0)))\n    .select(\n        ["shapValues", "outlierScore"] + inputCols + [timestampColumn, "predictedLabel"]\n    )\n    .withColumn("sensor_1_localimp", F.col("shapValues")[1])\n    .withColumn("sensor_2_localimp", F.col("shapValues")[2])\n    .withColumn("sensor_3_localimp", F.col("shapValues")[3])\n)\n')),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"shaps_local = shaps.toPandas()\nshaps_local\n")),(0,o.kt)("p",null,"Retrieve local feature importances"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'local_importance_values = shaps_local[["shapValues"]]\neval_data = shaps_local[inputCols]\n')),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"# Removing the first element in the list of local importance values (this is the base value or mean output of the background dataset)\nlist_local_importance_values = local_importance_values.values.tolist()\nconverted_importance_values = []\nbias = []\nfor classarray in list_local_importance_values:\n    for rowarray in classarray:\n        converted_list = rowarray.tolist()\n        bias.append(converted_list[0])\n        # remove the bias from local importance values\n        del converted_list[0]\n        converted_importance_values.append(converted_list)\n")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"from interpret_community.adapter import ExplanationAdapter\n\nadapter = ExplanationAdapter(inputCols, classification=False)\nglobal_explanation = adapter.create_global(\n    converted_importance_values, eval_data, expected_values=bias\n)\n")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"# view the global importance values\nglobal_explanation.global_importance_values\n")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"# view the local importance values\nglobal_explanation.local_importance_values\n")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'# Defining a wrapper class with predict method for creating the Explanation Dashboard\n\n\nclass wrapper(object):\n    def __init__(self, model):\n        self.model = model\n\n    def predict(self, data):\n        sparkdata = spark.createDataFrame(data)\n        return (\n            model.transform(sparkdata)\n            .select("outlierScore")\n            .toPandas()\n            .values.flatten()\n            .tolist()\n        )\n')),(0,o.kt)("h2",{id:"visualize-results"},"Visualize results"),(0,o.kt)("p",null,"Visualize anomaly results and feature contribution scores (derived from local feature importance)"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'import matplotlib.pyplot as plt\n\n\ndef visualize(rdf):\n    anoms = list(rdf["predictedLabel"] == 1)\n\n    fig = plt.figure(figsize=(26, 12))\n\n    ax = fig.add_subplot(611)\n    ax.title.set_text(f"Multivariate Anomaly Detection Results")\n    ax.plot(\n        rdf[timestampColumn],\n        rdf["sensor_1"],\n        color="tab:orange",\n        line,\n        linewidth=2,\n        label="sensor_1",\n    )\n    ax.grid(axis="y")\n    _, _, ymin, ymax = plt.axis()\n    ax.vlines(\n        rdf[timestampColumn][anoms],\n        ymin=ymin,\n        ymax=ymax,\n        color="tab:red",\n        alpha=0.2,\n        linewidth=6,\n    )\n    ax.tick_params(axis="x", which="both", bottom=False, labelbottom=False)\n    ax.set_ylabel("sensor1_value")\n    ax.legend()\n\n    ax = fig.add_subplot(612, sharex=ax)\n    ax.plot(\n        rdf[timestampColumn],\n        rdf["sensor_2"],\n        color="tab:green",\n        line,\n        linewidth=2,\n        label="sensor_2",\n    )\n    ax.grid(axis="y")\n    _, _, ymin, ymax = plt.axis()\n    ax.vlines(\n        rdf[timestampColumn][anoms],\n        ymin=ymin,\n        ymax=ymax,\n        color="tab:red",\n        alpha=0.2,\n        linewidth=6,\n    )\n    ax.tick_params(axis="x", which="both", bottom=False, labelbottom=False)\n    ax.set_ylabel("sensor2_value")\n    ax.legend()\n\n    ax = fig.add_subplot(613, sharex=ax)\n    ax.plot(\n        rdf[timestampColumn],\n        rdf["sensor_3"],\n        color="tab:purple",\n        line,\n        linewidth=2,\n        label="sensor_3",\n    )\n    ax.grid(axis="y")\n    _, _, ymin, ymax = plt.axis()\n    ax.vlines(\n        rdf[timestampColumn][anoms],\n        ymin=ymin,\n        ymax=ymax,\n        color="tab:red",\n        alpha=0.2,\n        linewidth=6,\n    )\n    ax.tick_params(axis="x", which="both", bottom=False, labelbottom=False)\n    ax.set_ylabel("sensor3_value")\n    ax.legend()\n\n    ax = fig.add_subplot(614, sharex=ax)\n    ax.tick_params(axis="x", which="both", bottom=False, labelbottom=False)\n    ax.plot(\n        rdf[timestampColumn],\n        rdf["outlierScore"],\n        color="black",\n        line,\n        linewidth=2,\n        label="Outlier score",\n    )\n    ax.set_ylabel("outlier score")\n    ax.grid(axis="y")\n    ax.legend()\n\n    ax = fig.add_subplot(615, sharex=ax)\n    ax.tick_params(axis="x", which="both", bottom=False, labelbottom=False)\n    ax.bar(\n        rdf[timestampColumn],\n        rdf["sensor_1_localimp"].abs(),\n        width=2,\n        color="tab:orange",\n        label="sensor_1",\n    )\n    ax.bar(\n        rdf[timestampColumn],\n        rdf["sensor_2_localimp"].abs(),\n        width=2,\n        color="tab:green",\n        label="sensor_2",\n        bottom=rdf["sensor_1_localimp"].abs(),\n    )\n    ax.bar(\n        rdf[timestampColumn],\n        rdf["sensor_3_localimp"].abs(),\n        width=2,\n        color="tab:purple",\n        label="sensor_3",\n        bottom=rdf["sensor_1_localimp"].abs() + rdf["sensor_2_localimp"].abs(),\n    )\n    ax.set_ylabel("Contribution scores")\n    ax.grid(axis="y")\n    ax.legend()\n\n    plt.show()\n')),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"visualize(shaps_local)\n")),(0,o.kt)("p",null,"When you run the cell above, you will see the following plots:"),(0,o.kt)("p",null,(0,o.kt)("img",{parentName:"p",src:"https://mmlspark.blob.core.windows.net/graphics/notebooks/mvad_results_local_importances.jpg",alt:null})),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"The first 3 plots above show the sensor time series data in the inference window, in orange, green, purple and blue. The red vertical lines show the detected anomalies (",(0,o.kt)("inlineCode",{parentName:"li"},"prediction")," = 1). "),(0,o.kt)("li",{parentName:"ul"},"The fourth plot shows the outlierScore of all the points, with the ",(0,o.kt)("inlineCode",{parentName:"li"},"minOutlierScore")," threshold shown by the dotted red horizontal line."),(0,o.kt)("li",{parentName:"ul"},"The last plot shows the contribution scores of each sensor to the ",(0,o.kt)("inlineCode",{parentName:"li"},"outlierScore")," for that point.")),(0,o.kt)("p",null,"Plot aggregate feature importance"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'plt.figure(figsize=(10, 7))\nplt.bar(inputCols, global_explanation.global_importance_values)\nplt.ylabel("global importance values")\n')),(0,o.kt)("p",null,"When you run the cell above, you will see the following global feature importance plot:"),(0,o.kt)("p",null,(0,o.kt)("img",{parentName:"p",src:"https://mmlspark.blob.core.windows.net/graphics/notebooks/global-feature-importance.jpg",alt:null})),(0,o.kt)("p",null,"Visualize the explanation in the ExplanationDashboard from ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/microsoft/responsible-ai-widgets"},"https://github.com/microsoft/responsible-ai-widgets"),"."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"# View the model explanation in the ExplanationDashboard\nfrom raiwidgets import ExplanationDashboard\n\nExplanationDashboard(global_explanation, wrapper(model), dataset=eval_data)\n")))}c.isMDXComponent=!0}}]);