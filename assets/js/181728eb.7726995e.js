"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[60768],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>d});var l=n(67294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function r(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);t&&(l=l.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,l)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?r(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):r(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function i(e,t){if(null==e)return{};var n,l,a=function(e,t){if(null==e)return{};var n,l,a={},r=Object.keys(e);for(l=0;l<r.length;l++)n=r[l],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(l=0;l<r.length;l++)n=r[l],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var s=l.createContext({}),m=function(e){var t=l.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},p=function(e){var t=m(e.components);return l.createElement(s.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return l.createElement(l.Fragment,{},t)}},u=l.forwardRef((function(e,t){var n=e.components,a=e.mdxType,r=e.originalType,s=e.parentName,p=i(e,["components","mdxType","originalType","parentName"]),u=m(n),d=a,f=u["".concat(s,".").concat(d)]||u[d]||c[d]||r;return n?l.createElement(f,o(o({ref:t},p),{},{components:n})):l.createElement(f,o({ref:t},p))}));function d(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var r=n.length,o=new Array(r);o[0]=u;var i={};for(var s in t)hasOwnProperty.call(t,s)&&(i[s]=t[s]);i.originalType=e,i.mdxType="string"==typeof e?e:a,o[1]=i;for(var m=2;m<r;m++)o[m]=n[m];return l.createElement.apply(null,o)}return l.createElement.apply(null,n)}u.displayName="MDXCreateElement"},63965:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>o,default:()=>c,frontMatter:()=>r,metadata:()=>i,toc:()=>m});var l=n(83117),a=(n(67294),n(3905));const r={title:"Overview",description:"MLflow support of SynapseML"},o=void 0,i={unversionedId:"Use with MLFlow/Overview",id:"version-1.0.6/Use with MLFlow/Overview",title:"Overview",description:"MLflow support of SynapseML",source:"@site/versioned_docs/version-1.0.6/Use with MLFlow/Overview.md",sourceDirName:"Use with MLFlow",slug:"/Use with MLFlow/Overview",permalink:"/SynapseML/docs/Use with MLFlow/Overview",draft:!1,tags:[],version:"1.0.6",frontMatter:{title:"Overview",description:"MLflow support of SynapseML"},sidebar:"docs",previous:{title:"Quickstart - Exploring Art Across Cultures",permalink:"/SynapseML/docs/Explore Algorithms/Other Algorithms/Quickstart - Exploring Art Across Cultures"},next:{title:"Install",permalink:"/SynapseML/docs/Use with MLFlow/Install"}},s={},m=[{value:"What is MLflow",id:"what-is-mlflow",level:2},{value:"Installation",id:"installation",level:2},{value:"Install Mlflow on Databricks",id:"install-mlflow-on-databricks",level:3},{value:"Install Mlflow on Synapse",id:"install-mlflow-on-synapse",level:3},{value:"Create Azure Machine Learning Workspace",id:"create-azure-machine-learning-workspace",level:4},{value:"Create an Azure ML Linked Service",id:"create-an-azure-ml-linked-service",level:4},{value:"Auth Synapse Workspace",id:"auth-synapse-workspace",level:4},{value:"Use MLFlow in Synapse with Linked Service",id:"use-mlflow-in-synapse-with-linked-service",level:4},{value:"Use MLFlow in Synapse without a Linked Service",id:"use-mlflow-in-synapse-without-a-linked-service",level:4},{value:"MLFlow API Reference",id:"mlflow-api-reference",level:2},{value:"Examples",id:"examples",level:2},{value:"LightGBMClassifier",id:"lightgbmclassifier",level:3},{value:"Azure AI Services",id:"azure-ai-services",level:3}],p={toc:m};function c(e){let{components:t,...n}=e;return(0,a.kt)("wrapper",(0,l.Z)({},p,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h2",{id:"what-is-mlflow"},"What is MLflow"),(0,a.kt)("p",null,(0,a.kt)("a",{parentName:"p",href:"https://github.com/mlflow/mlflow"},"MLflow")," is a platform to streamline machine learning development, including tracking experiments, packaging code into reproducible runs, and sharing and deploying models. MLflow offers a set of lightweight APIs that can be used with any existing machine learning application or library, for instance TensorFlow, PyTorch, XGBoost, etc. It runs wherever you currently run ML code, for example, in notebooks, standalone applications or the cloud. MLflow's current components are:"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"https://mlflow.org/docs/latest/tracking.html"},"MLflow Tracking"),": An API to log parameters, code, and results in machine learning experiments and compare them using an interactive UI."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"https://mlflow.org/docs/latest/projects.html"},"MLflow Projects"),": A code packaging format for reproducible runs using Conda and Docker, so you can share your ML code with others."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"https://mlflow.org/docs/latest/models.html"},"MLflow Models"),": A model packaging format and tools that let you easily deploy the same model from any ML library for both batch and real-time scoring. It supports platforms such as Docker, Apache Spark, Azure ML and AWS SageMaker."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"https://mlflow.org/docs/latest/model-registry.html"},"MLflow Model Registry"),": A centralized model store, set of APIs, and UI, to collaboratively manage the full lifecycle of MLflow Models.")),(0,a.kt)("h2",{id:"installation"},"Installation"),(0,a.kt)("p",null,"Install MLflow from PyPI via ",(0,a.kt)("inlineCode",{parentName:"p"},"pip install mlflow")),(0,a.kt)("p",null,"MLflow requires ",(0,a.kt)("inlineCode",{parentName:"p"},"conda")," to be on the ",(0,a.kt)("inlineCode",{parentName:"p"},"PATH")," for the projects feature."),(0,a.kt)("p",null,"Learn more about MLflow on their ",(0,a.kt)("a",{parentName:"p",href:"https://github.com/mlflow/mlflow"},"GitHub page"),"."),(0,a.kt)("h3",{id:"install-mlflow-on-databricks"},"Install Mlflow on Databricks"),(0,a.kt)("p",null,"If you're using Databricks, install Mlflow with this command:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"# run this so that Mlflow is installed on workers besides driver\n%pip install mlflow\n")),(0,a.kt)("h3",{id:"install-mlflow-on-synapse"},"Install Mlflow on Synapse"),(0,a.kt)("p",null,"To log model with Mlflow, you need to create an Azure Machine Learning workspace and link it with your Synapse workspace."),(0,a.kt)("h4",{id:"create-azure-machine-learning-workspace"},"Create Azure Machine Learning Workspace"),(0,a.kt)("p",null,"Follow this document to create ",(0,a.kt)("a",{parentName:"p",href:"https://learn.microsoft.com/en-us/azure/machine-learning/quickstart-create-resources#create-the-workspace"},"AML workspace"),". You don't need to create compute instance and compute clusters."),(0,a.kt)("h4",{id:"create-an-azure-ml-linked-service"},"Create an Azure ML Linked Service"),(0,a.kt)("img",{src:"https://mmlspark.blob.core.windows.net/graphics/Documentation/ml_linked_service_1.png",width:"600"}),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"In the Synapse workspace, go to ",(0,a.kt)("strong",{parentName:"li"},"Manage")," -> ",(0,a.kt)("strong",{parentName:"li"},"External connections")," -> ",(0,a.kt)("strong",{parentName:"li"},"Linked services"),", select ",(0,a.kt)("strong",{parentName:"li"},"+ New")),(0,a.kt)("li",{parentName:"ul"},"Select the workspace you want to log the model in and create the linked service. You need the ",(0,a.kt)("strong",{parentName:"li"},"name of the linked service")," to set up connection.")),(0,a.kt)("h4",{id:"auth-synapse-workspace"},"Auth Synapse Workspace"),(0,a.kt)("img",{src:"https://mmlspark.blob.core.windows.net/graphics/Documentation/ml_linked_service_2.png",width:"600"}),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Go to the ",(0,a.kt)("strong",{parentName:"li"},"Azure Machine Learning workspace")," resource -> ",(0,a.kt)("strong",{parentName:"li"},"access control (IAM)")," -> ",(0,a.kt)("strong",{parentName:"li"},"Role assignment"),", select ",(0,a.kt)("strong",{parentName:"li"},"+ Add"),", choose ",(0,a.kt)("strong",{parentName:"li"},"Add role assignment")),(0,a.kt)("li",{parentName:"ul"},"Choose ",(0,a.kt)("strong",{parentName:"li"},"contributor"),", select next"),(0,a.kt)("li",{parentName:"ul"},"In members page, choose ",(0,a.kt)("strong",{parentName:"li"},"Managed identity"),", select  ",(0,a.kt)("strong",{parentName:"li"},"+ select members"),". Under ",(0,a.kt)("strong",{parentName:"li"},"managed identity"),", choose Synapse workspace. Under ",(0,a.kt)("strong",{parentName:"li"},"Select"),", choose the workspace you run your experiment on. Click ",(0,a.kt)("strong",{parentName:"li"},"Select"),", ",(0,a.kt)("strong",{parentName:"li"},"Review + assign"),".")),(0,a.kt)("h4",{id:"use-mlflow-in-synapse-with-linked-service"},"Use MLFlow in Synapse with Linked Service"),(0,a.kt)("p",null,"Set up connection"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'\n#AML\xa0workspace\xa0authentication\xa0using\xa0linked\xa0service\nfrom\xa0notebookutils.mssparkutils\xa0import\xa0azureML\nlinked_service_name = "YourLinkedServiceName"\nws\xa0=\xa0azureML.getWorkspace(linked_service_name)\nmlflow.set_tracking_uri(ws.get_mlflow_tracking_uri())\n\n#Set\xa0MLflow\xa0experiment.\xa0\nexperiment_name\xa0=\xa0"synapse-mlflow-experiment"\nmlflow.set_experiment(experiment_name)\xa0\n')),(0,a.kt)("h4",{id:"use-mlflow-in-synapse-without-a-linked-service"},"Use MLFlow in Synapse without a Linked Service"),(0,a.kt)("p",null,"Once you create an AML workspace, you can obtain the MLflow tracking URL directly. The AML start page is where you can locate the MLflow tracking URL."),(0,a.kt)("img",{src:"https://mmlspark.blob.core.windows.net/graphics/Documentation/mlflow_tracking_url.png",width:"600"}),'You can set it tracking url with ```python mlflow.set_tracking_uri("your mlflow tracking url") ```',(0,a.kt)("h2",{id:"mlflow-api-reference"},"MLFlow API Reference"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"https://www.mlflow.org/docs/latest/python_api/mlflow.spark.html#mlflow.spark.save_model"},"mlflow.spark.save_model")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"https://www.mlflow.org/docs/latest/python_api/mlflow.spark.html#mlflow.spark.log_model"},"mlflow.spark.log_model")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"https://www.mlflow.org/docs/latest/python_api/mlflow.spark.html#mlflow.spark.load_model"},"mlflow.spark.load_model")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"https://mlflow.org/docs/latest/python_api/mlflow.html#mlflow.log_metric"},"mlflow.log_metric"))),(0,a.kt)("h2",{id:"examples"},"Examples"),(0,a.kt)("h3",{id:"lightgbmclassifier"},"LightGBMClassifier"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'import mlflow\nfrom synapse.ml.featurize import Featurize\nfrom synapse.ml.lightgbm import *\nfrom synapse.ml.train import ComputeModelStatistics\n\nwith mlflow.start_run():\n\n    feature_columns = ["Number of times pregnant","Plasma glucose concentration a 2 hours in an oral glucose tolerance test",\n    "Diastolic blood pressure (mm Hg)","Triceps skin fold thickness (mm)","2-Hour serum insulin (mu U/ml)",\n    "Body mass index (weight in kg/(height in m)^2)","Diabetes pedigree function","Age (years)"]\n    df = spark.createDataFrame([\n        (0,131,66,40,0,34.3,0.196,22,1),\n        (7,194,68,28,0,35.9,0.745,41,1),\n        (3,139,54,0,0,25.6,0.402,22,1),\n        (6,134,70,23,130,35.4,0.542,29,1),\n        (9,124,70,33,402,35.4,0.282,34,0),\n        (0,93,100,39,72,43.4,1.021,35,0),\n        (4,110,76,20,100,28.4,0.118,27,0),\n        (2,127,58,24,275,27.7,1.6,25,0),\n        (0,104,64,37,64,33.6,0.51,22,1),\n        (2,120,54,0,0,26.8,0.455,27,0),\n        (7,178,84,0,0,39.9,0.331,41,1),\n        (2,88,58,26,16,28.4,0.766,22,0),\n        (1,91,64,24,0,29.2,0.192,21,0),\n        (10,101,76,48,180,32.9,0.171,63,0),\n        (5,73,60,0,0,26.8,0.268,27,0),\n        (3,158,70,30,328,35.5,0.344,35,1),\n        (2,105,75,0,0,23.3,0.56,53,0),\n        (12,84,72,31,0,29.7,0.297,46,1),\n        (9,119,80,35,0,29.0,0.263,29,1),\n        (6,93,50,30,64,28.7,0.356,23,0),\n        (1,126,60,0,0,30.1,0.349,47,1)\n    ], feature_columns+["labels"]).repartition(2)\n\n\n    featurize = (Featurize()\n    .setOutputCol("features")\n    .setInputCols(feature_columns)\n    .setOneHotEncodeCategoricals(True)\n    .setNumFeatures(4096))\n\n    df_trans = featurize.fit(df).transform(df)\n\n    lightgbm_classifier = (LightGBMClassifier()\n            .setFeaturesCol("features")\n            .setRawPredictionCol("rawPrediction")\n            .setDefaultListenPort(12402)\n            .setNumLeaves(5)\n            .setNumIterations(10)\n            .setObjective("binary")\n            .setLabelCol("labels")\n            .setLeafPredictionCol("leafPrediction")\n            .setFeaturesShapCol("featuresShap"))\n\n    lightgbm_model = lightgbm_classifier.fit(df_trans)\n\n    # Use mlflow.spark.save_model to save the model to your path\n    mlflow.spark.save_model(lightgbm_model, "lightgbm_model")\n    # Use mlflow.spark.log_model to log the model if you have a connected mlflow service\n    mlflow.spark.log_model(lightgbm_model, "lightgbm_model")\n\n    # Use mlflow.pyfunc.load_model to load model back as PyFuncModel and apply predict\n    prediction = mlflow.pyfunc.load_model("lightgbm_model").predict(df_trans.toPandas())\n    prediction = list(map(str, prediction))\n    mlflow.log_param("prediction", ",".join(prediction))\n\n    # Use mlflow.spark.load_model to load model back as PipelineModel and apply transform\n    predictions = mlflow.spark.load_model("lightgbm_model").transform(df_trans)\n    metrics = ComputeModelStatistics(evaluationMetric="classification", labelCol=\'labels\', scoredLabelsCol=\'prediction\').transform(predictions).collect()\n    mlflow.log_metric("accuracy", metrics[0][\'accuracy\'])\n')),(0,a.kt)("h3",{id:"azure-ai-services"},"Azure AI Services"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'import mlflow\nfrom synapse.ml.services import *\n\nwith mlflow.start_run():\n\n    text_key = "YOUR_COG_SERVICE_SUBSCRIPTION_KEY"\n    df = spark.createDataFrame([\n    ("I am so happy today, its sunny!", "en-US"),\n    ("I am frustrated by this rush hour traffic", "en-US"),\n    ("The cognitive services on spark aint bad", "en-US"),\n    ], ["text", "language"])\n\n    sentiment_model = (TextSentiment()\n                .setSubscriptionKey(text_key)\n                .setLocation("eastus")\n                .setTextCol("text")\n                .setOutputCol("prediction")\n                .setErrorCol("error")\n                .setLanguageCol("language"))\n\n    display(sentiment_model.transform(df))\n\n    mlflow.spark.save_model(sentiment_model, "sentiment_model")\n    mlflow.spark.log_model(sentiment_model, "sentiment_model")\n\n    output_df = mlflow.spark.load_model("sentiment_model").transform(df)\n    display(output_df)\n\n    # In order to call the predict function successfully you need to specify the\n    # outputCol name as `prediction`\n    prediction = mlflow.pyfunc.load_model("sentiment_model").predict(df.toPandas())\n    prediction = list(map(str, prediction))\n    mlflow.log_param("prediction", ",".join(prediction))\n')))}c.isMDXComponent=!0}}]);