"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[40377],{3905:(e,n,t)=>{t.d(n,{Zo:()=>m,kt:()=>g});var r=t(67294);function a(e,n,t){return n in e?Object.defineProperty(e,n,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[n]=t,e}function l(e,n){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);n&&(r=r.filter((function(n){return Object.getOwnPropertyDescriptor(e,n).enumerable}))),t.push.apply(t,r)}return t}function s(e){for(var n=1;n<arguments.length;n++){var t=null!=arguments[n]?arguments[n]:{};n%2?l(Object(t),!0).forEach((function(n){a(e,n,t[n])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):l(Object(t)).forEach((function(n){Object.defineProperty(e,n,Object.getOwnPropertyDescriptor(t,n))}))}return e}function i(e,n){if(null==e)return{};var t,r,a=function(e,n){if(null==e)return{};var t,r,a={},l=Object.keys(e);for(r=0;r<l.length;r++)t=l[r],n.indexOf(t)>=0||(a[t]=e[t]);return a}(e,n);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(r=0;r<l.length;r++)t=l[r],n.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(a[t]=e[t])}return a}var o=r.createContext({}),p=function(e){var n=r.useContext(o),t=n;return e&&(t="function"==typeof e?e(n):s(s({},n),e)),t},m=function(e){var n=p(e.components);return r.createElement(o.Provider,{value:n},e.children)},u={inlineCode:"code",wrapper:function(e){var n=e.children;return r.createElement(r.Fragment,{},n)}},c=r.forwardRef((function(e,n){var t=e.components,a=e.mdxType,l=e.originalType,o=e.parentName,m=i(e,["components","mdxType","originalType","parentName"]),c=p(t),g=a,d=c["".concat(o,".").concat(g)]||c[g]||u[g]||l;return t?r.createElement(d,s(s({ref:n},m),{},{components:t})):r.createElement(d,s({ref:n},m))}));function g(e,n){var t=arguments,a=n&&n.mdxType;if("string"==typeof e||a){var l=t.length,s=new Array(l);s[0]=c;var i={};for(var o in n)hasOwnProperty.call(n,o)&&(i[o]=n[o]);i.originalType=e,i.mdxType="string"==typeof e?e:a,s[1]=i;for(var p=2;p<l;p++)s[p]=t[p];return r.createElement.apply(null,s)}return r.createElement.apply(null,t)}c.displayName="MDXCreateElement"},34409:(e,n,t)=>{t.r(n),t.d(n,{assets:()=>o,contentTitle:()=>s,default:()=>u,frontMatter:()=>l,metadata:()=>i,toc:()=>p});var r=t(83117),a=(t(67294),t(3905));const l={title:"Image Explainers",hide_title:!0,status:"stable"},s=void 0,i={unversionedId:"Explore Algorithms/Responsible AI/Image Explainers",id:"version-1.0.6/Explore Algorithms/Responsible AI/Image Explainers",title:"Image Explainers",description:"Interpretability - Image Explainers",source:"@site/versioned_docs/version-1.0.6/Explore Algorithms/Responsible AI/Image Explainers.md",sourceDirName:"Explore Algorithms/Responsible AI",slug:"/Explore Algorithms/Responsible AI/Image Explainers",permalink:"/SynapseML/docs/1.0.6/Explore Algorithms/Responsible AI/Image Explainers",draft:!1,tags:[],version:"1.0.6",frontMatter:{title:"Image Explainers",hide_title:!0,status:"stable"},sidebar:"docs",previous:{title:"Text Explainers",permalink:"/SynapseML/docs/1.0.6/Explore Algorithms/Responsible AI/Text Explainers"},next:{title:"PDP and ICE Explainers",permalink:"/SynapseML/docs/1.0.6/Explore Algorithms/Responsible AI/PDP and ICE Explainers"}},o={},p=[{value:"Interpretability - Image Explainers",id:"interpretability---image-explainers",level:2}],m={toc:p};function u(e){let{components:n,...t}=e;return(0,a.kt)("wrapper",(0,r.Z)({},m,t,{components:n,mdxType:"MDXLayout"}),(0,a.kt)("h2",{id:"interpretability---image-explainers"},"Interpretability - Image Explainers"),(0,a.kt)("p",null,"In this example, we use LIME and Kernel SHAP explainers to explain the ResNet50 model's multi-class output of an image."),(0,a.kt)("p",null,"First we import the packages and define some UDFs and a plotting function we will need later."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.explainers import *\nfrom synapse.ml.onnx import ONNXModel\nfrom synapse.ml.opencv import ImageTransformer\nfrom synapse.ml.io import *\nfrom pyspark.ml import Pipeline\nfrom pyspark.sql.functions import *\nfrom pyspark.sql.types import *\nimport numpy as np\nimport urllib.request\nimport matplotlib.pyplot as plt\nfrom PIL import Image\nfrom synapse.ml.core.platform import *\n\n\nvec_slice = udf(\n    lambda vec, indices: (vec.toArray())[indices].tolist(), ArrayType(FloatType())\n)\narg_top_k = udf(\n    lambda vec, k: (-vec.toArray()).argsort()[:k].tolist(), ArrayType(IntegerType())\n)\n\n\ndef downloadBytes(url: str):\n    with urllib.request.urlopen(url) as url:\n        barr = url.read()\n        return barr\n\n\ndef rotate_color_channel(bgr_image_array, height, width, nChannels):\n    B, G, R, *_ = np.asarray(bgr_image_array).reshape(height, width, nChannels).T\n    rgb_image_array = np.array((R, G, B)).T\n    return rgb_image_array\n\n\ndef plot_superpixels(image_rgb_array, sp_clusters, weights, green_threshold=99):\n    superpixels = sp_clusters\n    green_value = np.percentile(weights, green_threshold)\n    img = Image.fromarray(image_rgb_array, mode="RGB").convert("RGBA")\n    image_array = np.asarray(img).copy()\n    for (sp, v) in zip(superpixels, weights):\n        if v > green_value:\n            for (x, y) in sp:\n                image_array[y, x, 1] = 255\n                image_array[y, x, 3] = 200\n    plt.clf()\n    plt.imshow(image_array)\n    plt.show()\n')),(0,a.kt)("p",null,"Create a dataframe for a testing image, and use the ResNet50 ONNX model to infer the image."),(0,a.kt)("p",null,'The result shows 39.6% probability of "violin" (889), and 38.4% probability of "upright piano" (881).'),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.io import *\n\nimage_df = spark.read.image().load(\n    "wasbs://publicwasb@mmlspark.blob.core.windows.net/explainers/images/david-lusvardi-dWcUncxocQY-unsplash.jpg"\n)\ndisplay(image_df)\n\n# Rotate the image array from BGR into RGB channels for visualization later.\nrow = image_df.select(\n    "image.height", "image.width", "image.nChannels", "image.data"\n).head()\nlocals().update(row.asDict())\nrgb_image_array = rotate_color_channel(data, height, width, nChannels)\n\n# Download the ONNX model\nmodelPayload = downloadBytes(\n    "https://mmlspark.blob.core.windows.net/publicwasb/ONNXModels/resnet50-v2-7.onnx"\n)\n\nfeaturizer = (\n    ImageTransformer(inputCol="image", outputCol="features")\n    .resize(224, True)\n    .centerCrop(224, 224)\n    .normalize(\n        mean=[0.485, 0.456, 0.406],\n        std=[0.229, 0.224, 0.225],\n        color_scale_factor=1 / 255,\n    )\n    .setTensorElementType(FloatType())\n)\n\nonnx = (\n    ONNXModel()\n    .setModelPayload(modelPayload)\n    .setFeedDict({"data": "features"})\n    .setFetchDict({"rawPrediction": "resnetv24_dense0_fwd"})\n    .setSoftMaxDict({"rawPrediction": "probability"})\n    .setMiniBatchSize(1)\n)\n\nmodel = Pipeline(stages=[featurizer, onnx]).fit(image_df)\n')),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'predicted = (\n    model.transform(image_df)\n    .withColumn("top2pred", arg_top_k(col("probability"), lit(2)))\n    .withColumn("top2prob", vec_slice(col("probability"), col("top2pred")))\n)\n\ndisplay(predicted.select("top2pred", "top2prob"))\n')),(0,a.kt)("p",null,"First we use the LIME image explainer to explain the model's top 2 classes' probabilities."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'lime = (\n    ImageLIME()\n    .setModel(model)\n    .setOutputCol("weights")\n    .setInputCol("image")\n    .setCellSize(150.0)\n    .setModifier(50.0)\n    .setNumSamples(500)\n    .setTargetCol("probability")\n    .setTargetClassesCol("top2pred")\n    .setSamplingFraction(0.7)\n)\n\nlime_result = (\n    lime.transform(predicted)\n    .withColumn("weights_violin", col("weights").getItem(0))\n    .withColumn("weights_piano", col("weights").getItem(1))\n    .cache()\n)\n\ndisplay(lime_result.select(col("weights_violin"), col("weights_piano")))\nlime_row = lime_result.head()\n')),(0,a.kt)("p",null,'We plot the LIME weights for "violin" output and "upright piano" output.'),(0,a.kt)("p",null,"Green areas are superpixels with LIME weights above 95 percentile."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'plot_superpixels(\n    rgb_image_array,\n    lime_row["superpixels"]["clusters"],\n    list(lime_row["weights_violin"]),\n    95,\n)\nplot_superpixels(\n    rgb_image_array,\n    lime_row["superpixels"]["clusters"],\n    list(lime_row["weights_piano"]),\n    95,\n)\n')),(0,a.kt)("p",null,"Your results will look like:"),(0,a.kt)("img",{src:"https://mmlspark.blob.core.windows.net/graphics/explainers/image-lime-20210811.png"}),(0,a.kt)("p",null,"Then we use the Kernel SHAP image explainer to explain the model's top 2 classes' probabilities."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'shap = (\n    ImageSHAP()\n    .setModel(model)\n    .setOutputCol("shaps")\n    .setSuperpixelCol("superpixels")\n    .setInputCol("image")\n    .setCellSize(150.0)\n    .setModifier(50.0)\n    .setNumSamples(500)\n    .setTargetCol("probability")\n    .setTargetClassesCol("top2pred")\n)\n\nshap_result = (\n    shap.transform(predicted)\n    .withColumn("shaps_violin", col("shaps").getItem(0))\n    .withColumn("shaps_piano", col("shaps").getItem(1))\n    .cache()\n)\n\ndisplay(shap_result.select(col("shaps_violin"), col("shaps_piano")))\nshap_row = shap_result.head()\n')),(0,a.kt)("p",null,'We plot the SHAP values for "piano" output and "cell" output.'),(0,a.kt)("p",null,"Green areas are superpixels with SHAP values above 95 percentile."),(0,a.kt)("blockquote",null,(0,a.kt)("p",{parentName:"blockquote"},"Notice that we drop the base value from the SHAP output before rendering the superpixels. The base value is the model output for the background (all black) image.")),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'plot_superpixels(\n    rgb_image_array,\n    shap_row["superpixels"]["clusters"],\n    list(shap_row["shaps_violin"][1:]),\n    95,\n)\nplot_superpixels(\n    rgb_image_array,\n    shap_row["superpixels"]["clusters"],\n    list(shap_row["shaps_piano"][1:]),\n    95,\n)\n')),(0,a.kt)("p",null,"Your results will look like:"),(0,a.kt)("img",{src:"https://mmlspark.blob.core.windows.net/graphics/explainers/image-shap-20210811.png"}))}u.isMDXComponent=!0}}]);