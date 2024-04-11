"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[28465],{3905:(e,t,r)=>{r.d(t,{Zo:()=>c,kt:()=>d});var n=r(67294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function i(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function s(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?i(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function o(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},i=Object.keys(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var l=n.createContext({}),p=function(e){var t=n.useContext(l),r=t;return e&&(r="function"==typeof e?e(t):s(s({},t),e)),r},c=function(e){var t=p(e.components);return n.createElement(l.Provider,{value:t},e.children)},m={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},u=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,i=e.originalType,l=e.parentName,c=o(e,["components","mdxType","originalType","parentName"]),u=p(r),d=a,y=u["".concat(l,".").concat(d)]||u[d]||m[d]||i;return r?n.createElement(y,s(s({ref:t},c),{},{components:r})):n.createElement(y,s({ref:t},c))}));function d(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=r.length,s=new Array(i);s[0]=u;var o={};for(var l in t)hasOwnProperty.call(t,l)&&(o[l]=t[l]);o.originalType=e,o.mdxType="string"==typeof e?e:a,s[1]=o;for(var p=2;p<i;p++)s[p]=r[p];return n.createElement.apply(null,s)}return n.createElement.apply(null,r)}u.displayName="MDXCreateElement"},31750:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>l,contentTitle:()=>s,default:()=>m,frontMatter:()=>i,metadata:()=>o,toc:()=>p});var n=r(83117),a=(r(67294),r(3905));const i={title:"Quickstart - Deploying a Classifier",hide_title:!0,status:"stable"},s=void 0,o={unversionedId:"Deploy Models/Quickstart - Deploying a Classifier",id:"version-1.0.3/Deploy Models/Quickstart - Deploying a Classifier",title:"Quickstart - Deploying a Classifier",description:"Model Deployment with Spark Serving",source:"@site/versioned_docs/version-1.0.3/Deploy Models/Quickstart - Deploying a Classifier.md",sourceDirName:"Deploy Models",slug:"/Deploy Models/Quickstart - Deploying a Classifier",permalink:"/SynapseML/docs/1.0.3/Deploy Models/Quickstart - Deploying a Classifier",draft:!1,tags:[],version:"1.0.3",frontMatter:{title:"Quickstart - Deploying a Classifier",hide_title:!0,status:"stable"},sidebar:"docs",previous:{title:"About",permalink:"/SynapseML/docs/1.0.3/Deploy Models/Overview"},next:{title:"Contributor Guide",permalink:"/SynapseML/docs/1.0.3/Reference/Contributor Guide"}},l={},p=[{value:"Model Deployment with Spark Serving",id:"model-deployment-with-spark-serving",level:2}],c={toc:p};function m(e){let{components:t,...r}=e;return(0,a.kt)("wrapper",(0,n.Z)({},c,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h2",{id:"model-deployment-with-spark-serving"},"Model Deployment with Spark Serving"),(0,a.kt)("p",null,"In this example, we try to predict incomes from the ",(0,a.kt)("em",{parentName:"p"},"Adult Census")," dataset. Then we will use Spark serving to deploy it as a realtime web service.\nFirst, we import needed packages:"),(0,a.kt)("p",null,"Now let's read the data and split it to train and test sets:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'data = spark.read.parquet(\n    "wasbs://publicwasb@mmlspark.blob.core.windows.net/AdultCensusIncome.parquet"\n)\ndata = data.select(["education", "marital-status", "hours-per-week", "income"])\ntrain, test = data.randomSplit([0.75, 0.25], seed=123)\ntrain.limit(10).toPandas()\n')),(0,a.kt)("p",null,(0,a.kt)("inlineCode",{parentName:"p"},"TrainClassifier")," can be used to initialize and fit a model, it wraps SparkML classifiers.\nYou can use ",(0,a.kt)("inlineCode",{parentName:"p"},"help(synapse.ml.TrainClassifier)")," to view the different parameters."),(0,a.kt)("p",null,"Note that it implicitly converts the data into the format expected by the algorithm. More specifically it:\ntokenizes, hashes strings, one-hot encodes categorical variables, assembles the features into a vector\netc.  The parameter ",(0,a.kt)("inlineCode",{parentName:"p"},"numFeatures")," controls the number of hashed features."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.train import TrainClassifier\nfrom pyspark.ml.classification import LogisticRegression\n\nmodel = TrainClassifier(\n    model=LogisticRegression(), labelCol="income", numFeatures=256\n).fit(train)\n')),(0,a.kt)("p",null,"After the model is trained, we score it against the test dataset and view metrics."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},"from synapse.ml.train import ComputeModelStatistics, TrainedClassifierModel\n\nprediction = model.transform(test)\nprediction.printSchema()\n")),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},"metrics = ComputeModelStatistics().transform(prediction)\nmetrics.limit(10).toPandas()\n")),(0,a.kt)("p",null,"First, we will define the webservice input/output.\nFor more information, you can visit the ",(0,a.kt)("a",{parentName:"p",href:"https://github.com/Microsoft/SynapseML/blob/master/docs/mmlspark-serving.md"},"documentation for Spark Serving")),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'from pyspark.sql.types import *\nfrom synapse.ml.io import *\nimport uuid\n\nserving_inputs = (\n    spark.readStream.server()\n    .address("localhost", 8898, "my_api")\n    .option("name", "my_api")\n    .load()\n    .parseRequest("my_api", test.schema)\n)\n\nserving_outputs = model.transform(serving_inputs).makeReply("prediction")\n\nserver = (\n    serving_outputs.writeStream.server()\n    .replyTo("my_api")\n    .queryName("my_query")\n    .option("checkpointLocation", "file:///tmp/checkpoints-{}".format(uuid.uuid1()))\n    .start()\n)\n')),(0,a.kt)("p",null,"Test the webservice"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'import requests\n\ndata = \'{"education":" 10th","marital-status":"Divorced","hours-per-week":40.0}\'\nr = requests.post(data=data, url="http://localhost:8898/my_api")\nprint("Response {}".format(r.text))\n')),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'import requests\n\ndata = \'{"education":" Masters","marital-status":"Married-civ-spouse","hours-per-week":40.0}\'\nr = requests.post(data=data, url="http://localhost:8898/my_api")\nprint("Response {}".format(r.text))\n')),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},"import time\n\ntime.sleep(20)  # wait for server to finish setting up (just to be safe)\nserver.stop()\n")),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},"")))}m.isMDXComponent=!0}}]);