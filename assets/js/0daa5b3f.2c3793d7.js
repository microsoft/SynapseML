"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[5024],{3905:function(e,t,n){n.d(t,{Zo:function(){return m},kt:function(){return c}});var r=n(67294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function l(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function i(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},o=Object.keys(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var s=r.createContext({}),p=function(e){var t=r.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):l(l({},t),e)),n},m=function(e){var t=p(e.components);return r.createElement(s.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},d=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,o=e.originalType,s=e.parentName,m=i(e,["components","mdxType","originalType","parentName"]),d=p(n),c=a,f=d["".concat(s,".").concat(c)]||d[c]||u[c]||o;return n?r.createElement(f,l(l({ref:t},m),{},{components:n})):r.createElement(f,l({ref:t},m))}));function c(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=n.length,l=new Array(o);l[0]=d;var i={};for(var s in t)hasOwnProperty.call(t,s)&&(i[s]=t[s]);i.originalType=e,i.mdxType="string"==typeof e?e:a,l[1]=i;for(var p=2;p<o;p++)l[p]=n[p];return r.createElement.apply(null,l)}return r.createElement.apply(null,n)}d.displayName="MDXCreateElement"},73426:function(e,t,n){n.r(t),n.d(t,{assets:function(){return m},contentTitle:function(){return s},default:function(){return c},frontMatter:function(){return i},metadata:function(){return p},toc:function(){return u}});var r=n(83117),a=n(80102),o=(n(67294),n(3905)),l=["components"],i={title:"Quickstart - ONNX Model Inference",hide_title:!0,status:"stable"},s="ONNX Inference on Spark",p={unversionedId:"Explore Algorithms/Deep Learning/Quickstart - ONNX Model Inference",id:"version-0.11.4/Explore Algorithms/Deep Learning/Quickstart - ONNX Model Inference",title:"Quickstart - ONNX Model Inference",description:"In this example, you train a LightGBM model and convert the model to ONNX format. Once converted, you use the model to infer some testing data on Spark.",source:"@site/versioned_docs/version-0.11.4/Explore Algorithms/Deep Learning/Quickstart - ONNX Model Inference.md",sourceDirName:"Explore Algorithms/Deep Learning",slug:"/Explore Algorithms/Deep Learning/Quickstart - ONNX Model Inference",permalink:"/SynapseML/docs/0.11.4/Explore Algorithms/Deep Learning/Quickstart - ONNX Model Inference",draft:!1,tags:[],version:"0.11.4",frontMatter:{title:"Quickstart - ONNX Model Inference",hide_title:!0,status:"stable"},sidebar:"docs",previous:{title:"Quickstart - Fine-tune a Vision Classifier",permalink:"/SynapseML/docs/0.11.4/Explore Algorithms/Deep Learning/Quickstart - Fine-tune a Vision Classifier"},next:{title:"Quickstart - Transfer Learn for Image Classification",permalink:"/SynapseML/docs/0.11.4/Explore Algorithms/Deep Learning/Quickstart - Transfer Learn for Image Classification"}},m={},u=[{value:"Load the example data",id:"load-the-example-data",level:2},{value:"Use LightGBM to train a model",id:"use-lightgbm-to-train-a-model",level:2},{value:"Convert the model to ONNX format",id:"convert-the-model-to-onnx-format",level:2},{value:"Use the model for inference",id:"use-the-model-for-inference",level:2}],d={toc:u};function c(e){var t=e.components,n=(0,a.Z)(e,l);return(0,o.kt)("wrapper",(0,r.Z)({},d,n,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h1",{id:"onnx-inference-on-spark"},"ONNX Inference on Spark"),(0,o.kt)("p",null,"In this example, you train a LightGBM model and convert the model to ",(0,o.kt)("a",{parentName:"p",href:"https://onnx.ai/"},"ONNX")," format. Once converted, you use the model to infer some testing data on Spark."),(0,o.kt)("p",null,"This example uses the following Python packages and versions:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("inlineCode",{parentName:"li"},"onnxmltools==1.7.0")),(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("inlineCode",{parentName:"li"},"lightgbm==3.2.1"))),(0,o.kt)("h2",{id:"load-the-example-data"},"Load the example data"),(0,o.kt)("p",null,"To load the example data, add the following code examples to cells in your notebook and then run the cells:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"%pip install lightgbm\n")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'df = (\n    spark.read.format("csv")\n    .option("header", True)\n    .option("inferSchema", True)\n    .load(\n        "wasbs://publicwasb@mmlspark.blob.core.windows.net/company_bankruptcy_prediction_data.csv"\n    )\n)\n\ndisplay(df)\n')),(0,o.kt)("p",null,"The output should look similar to the following table, though the values and number of rows may differ:"),(0,o.kt)("table",null,(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Interest Coverage Ratio"),(0,o.kt)("th",{parentName:"tr",align:null},"Net Income Flag"),(0,o.kt)("th",{parentName:"tr",align:null},"Equity to Liability"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"0.5641"),(0,o.kt)("td",{parentName:"tr",align:null},"1.0"),(0,o.kt)("td",{parentName:"tr",align:null},"0.0165")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"0.5702"),(0,o.kt)("td",{parentName:"tr",align:null},"1.0"),(0,o.kt)("td",{parentName:"tr",align:null},"0.0208")),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"0.5673"),(0,o.kt)("td",{parentName:"tr",align:null},"1.0"),(0,o.kt)("td",{parentName:"tr",align:null},"0.0165")))),(0,o.kt)("h2",{id:"use-lightgbm-to-train-a-model"},"Use LightGBM to train a model"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'from pyspark.ml.feature import VectorAssembler\nfrom synapse.ml.lightgbm import LightGBMClassifier\n\nfeature_cols = df.columns[1:]\nfeaturizer = VectorAssembler(inputCols=feature_cols, outputCol="features")\n\ntrain_data = featurizer.transform(df)["Bankrupt?", "features"]\n\nmodel = (\n    LightGBMClassifier(featuresCol="features", labelCol="Bankrupt?")\n    .setEarlyStoppingRound(300)\n    .setLambdaL1(0.5)\n    .setNumIterations(1000)\n    .setNumThreads(-1)\n    .setMaxDeltaStep(0.5)\n    .setNumLeaves(31)\n    .setMaxDepth(-1)\n    .setBaggingFraction(0.7)\n    .setFeatureFraction(0.7)\n    .setBaggingFreq(2)\n    .setObjective("binary")\n    .setIsUnbalance(True)\n    .setMinSumHessianInLeaf(20)\n    .setMinGainToSplit(0.01)\n)\n\nmodel = model.fit(train_data)\n')),(0,o.kt)("h2",{id:"convert-the-model-to-onnx-format"},"Convert the model to ONNX format"),(0,o.kt)("p",null,"The following code exports the trained model to a LightGBM booster and then converts it to ONNX format:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},"from synapse.ml.core.platform import running_on_binder\n\nif running_on_binder():\n    from IPython import get_ipython\n")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'import lightgbm as lgb\nfrom lightgbm import Booster, LGBMClassifier\n\n\ndef convertModel(lgbm_model: LGBMClassifier or Booster, input_size: int) -> bytes:\n    from onnxmltools.convert import convert_lightgbm\n    from onnxconverter_common.data_types import FloatTensorType\n\n    initial_types = [("input", FloatTensorType([-1, input_size]))]\n    onnx_model = convert_lightgbm(\n        lgbm_model, initial_types=initial_types, target_opset=9\n    )\n    return onnx_model.SerializeToString()\n\n\nbooster_model_str = model.getLightGBMBooster().modelStr().get()\nbooster = lgb.Booster(model_str=booster_model_str)\nmodel_payload_ml = convertModel(booster, len(feature_cols))\n')),(0,o.kt)("p",null,"After conversion, load the ONNX payload into an ",(0,o.kt)("inlineCode",{parentName:"p"},"ONNXModel")," and inspect the model inputs and outputs:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.onnx import ONNXModel\n\nonnx_ml = ONNXModel().setModelPayload(model_payload_ml)\n\nprint("Model inputs:" + str(onnx_ml.getModelInputs()))\nprint("Model outputs:" + str(onnx_ml.getModelOutputs()))\n')),(0,o.kt)("p",null,"Map the model input to the input dataframe's column name (FeedDict), and map the output dataframe's column names to the model outputs (FetchDict)."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'onnx_ml = (\n    onnx_ml.setDeviceType("CPU")\n    .setFeedDict({"input": "features"})\n    .setFetchDict({"probability": "probabilities", "prediction": "label"})\n    .setMiniBatchSize(5000)\n)\n')),(0,o.kt)("h2",{id:"use-the-model-for-inference"},"Use the model for inference"),(0,o.kt)("p",null,"To perform inference with the model, the following code creates testing data and transforms the data through the ONNX model."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-python"},'from pyspark.ml.feature import VectorAssembler\nimport pandas as pd\nimport numpy as np\n\nn = 1000 * 1000\nm = 95\ntest = np.random.rand(n, m)\ntestPdf = pd.DataFrame(test)\ncols = list(map(str, testPdf.columns))\ntestDf = spark.createDataFrame(testPdf)\ntestDf = testDf.union(testDf).repartition(200)\ntestDf = (\n    VectorAssembler()\n    .setInputCols(cols)\n    .setOutputCol("features")\n    .transform(testDf)\n    .drop(*cols)\n    .cache()\n)\n\ndisplay(onnx_ml.transform(testDf))\n')),(0,o.kt)("p",null,"The output should look similar to the following table, though the values and number of rows may differ:"),(0,o.kt)("table",null,(0,o.kt)("thead",{parentName:"table"},(0,o.kt)("tr",{parentName:"thead"},(0,o.kt)("th",{parentName:"tr",align:null},"Index"),(0,o.kt)("th",{parentName:"tr",align:null},"Features"),(0,o.kt)("th",{parentName:"tr",align:null},"Prediction"),(0,o.kt)("th",{parentName:"tr",align:null},"Probability"))),(0,o.kt)("tbody",{parentName:"table"},(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"1"),(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},'"{"type":1,"values":[0.105...')),(0,o.kt)("td",{parentName:"tr",align:null},"0"),(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},'"{"0":0.835...'))),(0,o.kt)("tr",{parentName:"tbody"},(0,o.kt)("td",{parentName:"tr",align:null},"2"),(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},'"{"type":1,"values":[0.814...')),(0,o.kt)("td",{parentName:"tr",align:null},"0"),(0,o.kt)("td",{parentName:"tr",align:null},(0,o.kt)("inlineCode",{parentName:"td"},'"{"0":0.658...'))))))}c.isMDXComponent=!0}}]);