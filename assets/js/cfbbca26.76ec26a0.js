"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[56053],{3905:(e,t,r)=>{r.d(t,{Zo:()=>u,kt:()=>m});var n=r(67294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function i(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?i(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):i(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},i=Object.keys(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(n=0;n<i.length;n++)r=i[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var l=n.createContext({}),p=function(e){var t=n.useContext(l),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},u=function(e){var t=p(e.components);return n.createElement(l.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},c=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,i=e.originalType,l=e.parentName,u=s(e,["components","mdxType","originalType","parentName"]),c=p(r),m=a,f=c["".concat(l,".").concat(m)]||c[m]||d[m]||i;return r?n.createElement(f,o(o({ref:t},u),{},{components:r})):n.createElement(f,o({ref:t},u))}));function m(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=r.length,o=new Array(i);o[0]=c;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:a,o[1]=s;for(var p=2;p<i;p++)o[p]=r[p];return n.createElement.apply(null,o)}return n.createElement.apply(null,r)}c.displayName="MDXCreateElement"},51472:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>l,contentTitle:()=>o,default:()=>d,frontMatter:()=>i,metadata:()=>s,toc:()=>p});var n=r(83117),a=(r(67294),r(3905));const i={title:"Quickstart - Your First Models",hide_title:!0,status:"stable"},o="Build your first SynapseML models",s={unversionedId:"Get Started/Quickstart - Your First Models",id:"version-1.0.10/Get Started/Quickstart - Your First Models",title:"Quickstart - Your First Models",description:"This tutorial provides a brief introduction to SynapseML. In particular, we use SynapseML to create two different pipelines for sentiment analysis. The first pipeline combines a text featurization stage with LightGBM regression to predict ratings based on review text from a dataset containing book reviews from Amazon. The second pipeline shows how to use prebuilt models through the Azure AI Services to solve this problem without training data.",source:"@site/versioned_docs/version-1.0.10/Get Started/Quickstart - Your First Models.md",sourceDirName:"Get Started",slug:"/Get Started/Quickstart - Your First Models",permalink:"/SynapseML/docs/Get Started/Quickstart - Your First Models",draft:!1,tags:[],version:"1.0.10",frontMatter:{title:"Quickstart - Your First Models",hide_title:!0,status:"stable"},sidebar:"docs",previous:{title:"Set up Cognitive Services",permalink:"/SynapseML/docs/Get Started/Set up Cognitive Services"},next:{title:"Overview",permalink:"/SynapseML/docs/Explore Algorithms/LightGBM/Overview"}},l={},p=[{value:"Load a dataset",id:"load-a-dataset",level:2},{value:"Create the training pipeline",id:"create-the-training-pipeline",level:2},{value:"Predict the output of the test data",id:"predict-the-output-of-the-test-data",level:2},{value:"Use Azure AI services to transform data in one step",id:"use-azure-ai-services-to-transform-data-in-one-step",level:2}],u={toc:p};function d(e){let{components:t,...r}=e;return(0,a.kt)("wrapper",(0,n.Z)({},u,r,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h1",{id:"build-your-first-synapseml-models"},"Build your first SynapseML models"),(0,a.kt)("p",null,"This tutorial provides a brief introduction to SynapseML. In particular, we use SynapseML to create two different pipelines for sentiment analysis. The first pipeline combines a text featurization stage with LightGBM regression to predict ratings based on review text from a dataset containing book reviews from Amazon. The second pipeline shows how to use prebuilt models through the Azure AI Services to solve this problem without training data."),(0,a.kt)("h2",{id:"load-a-dataset"},"Load a dataset"),(0,a.kt)("p",null,"Load your dataset and split it into train and test sets."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'train, test = (\n    spark.read.parquet(\n        "wasbs://publicwasb@mmlspark.blob.core.windows.net/BookReviewsFromAmazon10K.parquet"\n    )\n    .limit(1000)\n    .cache()\n    .randomSplit([0.8, 0.2])\n)\n\ndisplay(train)\n')),(0,a.kt)("h2",{id:"create-the-training-pipeline"},"Create the training pipeline"),(0,a.kt)("p",null,"Create a pipeline that featurizes data using ",(0,a.kt)("inlineCode",{parentName:"p"},"TextFeaturizer")," from the ",(0,a.kt)("inlineCode",{parentName:"p"},"synapse.ml.featurize.text")," library and derives a rating using the ",(0,a.kt)("inlineCode",{parentName:"p"},"LightGBMRegressor")," function."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'from pyspark.ml import Pipeline\nfrom synapse.ml.featurize.text import TextFeaturizer\nfrom synapse.ml.lightgbm import LightGBMRegressor\n\nmodel = Pipeline(\n    stages=[\n        TextFeaturizer(inputCol="text", outputCol="features"),\n        LightGBMRegressor(featuresCol="features", labelCol="rating"),\n    ]\n).fit(train)\n')),(0,a.kt)("h2",{id:"predict-the-output-of-the-test-data"},"Predict the output of the test data"),(0,a.kt)("p",null,"Call the ",(0,a.kt)("inlineCode",{parentName:"p"},"transform")," function on the model to predict and display the output of the test data as a dataframe."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},"display(model.transform(test))\n")),(0,a.kt)("h2",{id:"use-azure-ai-services-to-transform-data-in-one-step"},"Use Azure AI services to transform data in one step"),(0,a.kt)("p",null,"Alternatively, for these kinds of tasks that have a prebuilt solution, you can use SynapseML's integration with Azure AI services to transform your data in one step."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.services.language import AnalyzeText\nfrom synapse.ml.core.platform import find_secret\n\nmodel = AnalyzeText(\n    textCol="text",\n    outputCol="sentiment",\n    kind="SentimentAnalysis",\n    subscriptionKey=find_secret(\n        secret_name="ai-services-api-key", keyvault="mmlspark-build-keys"\n    ),  # Replace the call to find_secret with your key as a python string.\n).setLocation("eastus")\n\ndisplay(model.transform(test))\n')))}d.isMDXComponent=!0}}]);