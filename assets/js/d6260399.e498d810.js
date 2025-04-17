"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[82784],{3905:(e,t,n)=>{n.d(t,{Zo:()=>u,kt:()=>g});var r=n(67294);function i(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function s(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?s(Object(n),!0).forEach((function(t){i(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):s(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function a(e,t){if(null==e)return{};var n,r,i=function(e,t){if(null==e)return{};var n,r,i={},s=Object.keys(e);for(r=0;r<s.length;r++)n=s[r],t.indexOf(n)>=0||(i[n]=e[n]);return i}(e,t);if(Object.getOwnPropertySymbols){var s=Object.getOwnPropertySymbols(e);for(r=0;r<s.length;r++)n=s[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(i[n]=e[n])}return i}var l=r.createContext({}),c=function(e){var t=r.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},u=function(e){var t=c(e.components);return r.createElement(l.Provider,{value:t},e.children)},m={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},p=r.forwardRef((function(e,t){var n=e.components,i=e.mdxType,s=e.originalType,l=e.parentName,u=a(e,["components","mdxType","originalType","parentName"]),p=c(n),g=i,h=p["".concat(l,".").concat(g)]||p[g]||m[g]||s;return n?r.createElement(h,o(o({ref:t},u),{},{components:n})):r.createElement(h,o({ref:t},u))}));function g(e,t){var n=arguments,i=t&&t.mdxType;if("string"==typeof e||i){var s=n.length,o=new Array(s);o[0]=p;var a={};for(var l in t)hasOwnProperty.call(t,l)&&(a[l]=t[l]);a.originalType=e,a.mdxType="string"==typeof e?e:i,o[1]=a;for(var c=2;c<s;c++)o[c]=n[c];return r.createElement.apply(null,o)}return r.createElement.apply(null,n)}p.displayName="MDXCreateElement"},43206:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>l,contentTitle:()=>o,default:()=>m,frontMatter:()=>s,metadata:()=>a,toc:()=>c});var r=n(83117),i=(n(67294),n(3905));const s={title:"Quickstart - Analyze Celebrity Quotes",hide_title:!0,status:"stable"},o="Celebrity Quote Analysis with The Azure AI Services",a={unversionedId:"Explore Algorithms/AI Services/Quickstart - Analyze Celebrity Quotes",id:"version-v1.0.11/Explore Algorithms/AI Services/Quickstart - Analyze Celebrity Quotes",title:"Quickstart - Analyze Celebrity Quotes",description:"Extracting celebrity quote images using Bing Image Search on Spark",source:"@site/versioned_docs/version-v1.0.11/Explore Algorithms/AI Services/Quickstart - Analyze Celebrity Quotes.md",sourceDirName:"Explore Algorithms/AI Services",slug:"/Explore Algorithms/AI Services/Quickstart - Analyze Celebrity Quotes",permalink:"/SynapseML/docs/Explore Algorithms/AI Services/Quickstart - Analyze Celebrity Quotes",draft:!1,tags:[],version:"v1.0.11",frontMatter:{title:"Quickstart - Analyze Celebrity Quotes",hide_title:!0,status:"stable"},sidebar:"docs",previous:{title:"Advanced Usage - Async, Batching, and Multi-Key",permalink:"/SynapseML/docs/Explore Algorithms/AI Services/Advanced Usage - Async, Batching, and Multi-Key"},next:{title:"Quickstart - Analyze Text",permalink:"/SynapseML/docs/Explore Algorithms/AI Services/Quickstart - Analyze Text"}},l={},c=[{value:"Extracting celebrity quote images using Bing Image Search on Spark",id:"extracting-celebrity-quote-images-using-bing-image-search-on-spark",level:3},{value:"Recognizing Images of Celebrities",id:"recognizing-images-of-celebrities",level:3},{value:"Reading the quote from the image.",id:"reading-the-quote-from-the-image",level:3},{value:"Understanding the Sentiment of the Quote",id:"understanding-the-sentiment-of-the-quote",level:3},{value:"Tying it all together",id:"tying-it-all-together",level:3}],u={toc:c};function m(e){let{components:t,...n}=e;return(0,i.kt)("wrapper",(0,r.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"celebrity-quote-analysis-with-the-azure-ai-services"},"Celebrity Quote Analysis with The Azure AI Services"),(0,i.kt)("img",{src:"https://mmlspark.blob.core.windows.net/graphics/SparkSummit2/cog_services.png",width:"800"}),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.services import *\nfrom pyspark.ml import PipelineModel\nfrom pyspark.sql.functions import col, udf\nfrom pyspark.ml.feature import SQLTransformer\nfrom synapse.ml.core.platform import find_secret\n\n# put your service keys here\nai_service_key = find_secret(\n    secret_name="ai-services-api-key", keyvault="mmlspark-build-keys"\n)\nai_service_location = "eastus"\nbing_search_key = find_secret(\n    secret_name="bing-search-key", keyvault="mmlspark-build-keys"\n)\n')),(0,i.kt)("h3",{id:"extracting-celebrity-quote-images-using-bing-image-search-on-spark"},"Extracting celebrity quote images using Bing Image Search on Spark"),(0,i.kt)("p",null,"Here we define two Transformers to extract celebrity quote images."),(0,i.kt)("img",{src:"https://mmlspark.blob.core.windows.net/graphics/Cog%20Service%20NB/step%201.png",width:"600"}),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'imgsPerBatch = 10  # the number of images Bing will return for each query\noffsets = [\n    (i * imgsPerBatch,) for i in range(100)\n]  # A list of offsets, used to page into the search results\nbingParameters = spark.createDataFrame(offsets, ["offset"])\n\nbingSearch = (\n    BingImageSearch()\n    .setSubscriptionKey(bing_search_key)\n    .setOffsetCol("offset")\n    .setQuery("celebrity quotes")\n    .setCount(imgsPerBatch)\n    .setOutputCol("images")\n)\n\n# Transformer to that extracts and flattens the richly structured output of Bing Image Search into a simple URL column\ngetUrls = BingImageSearch.getUrlTransformer("images", "url")\n')),(0,i.kt)("h3",{id:"recognizing-images-of-celebrities"},"Recognizing Images of Celebrities"),(0,i.kt)("p",null,"This block identifies the name of the celebrities for each of the images returned by the Bing Image Search."),(0,i.kt)("img",{src:"https://mmlspark.blob.core.windows.net/graphics/Cog%20Service%20NB/step%202.png",width:"600"}),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'celebs = (\n    RecognizeDomainSpecificContent()\n    .setSubscriptionKey(ai_service_key)\n    .setLocation(ai_service_location)\n    .setModel("celebrities")\n    .setImageUrlCol("url")\n    .setOutputCol("celebs")\n)\n\n# Extract the first celebrity we see from the structured response\nfirstCeleb = SQLTransformer(\n    statement="SELECT *, celebs.result.celebrities[0].name as firstCeleb FROM __THIS__"\n)\n')),(0,i.kt)("h3",{id:"reading-the-quote-from-the-image"},"Reading the quote from the image."),(0,i.kt)("p",null,"This stage performs OCR on the images to recognize the quotes."),(0,i.kt)("img",{src:"https://mmlspark.blob.core.windows.net/graphics/Cog%20Service%20NB/step%203.png",width:"600"}),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.stages import UDFTransformer\n\nrecognizeText = (\n    RecognizeText()\n    .setSubscriptionKey(ai_service_key)\n    .setLocation(ai_service_location)\n    .setImageUrlCol("url")\n    .setMode("Printed")\n    .setOutputCol("ocr")\n    .setConcurrency(5)\n)\n\n\ndef getTextFunction(ocrRow):\n    if ocrRow is None:\n        return None\n    return "\\n".join([line.text for line in ocrRow.recognitionResult.lines])\n\n\n# this transformer wil extract a simpler string from the structured output of recognize text\ngetText = (\n    UDFTransformer()\n    .setUDF(udf(getTextFunction))\n    .setInputCol("ocr")\n    .setOutputCol("text")\n)\n')),(0,i.kt)("h3",{id:"understanding-the-sentiment-of-the-quote"},"Understanding the Sentiment of the Quote"),(0,i.kt)("img",{src:"https://mmlspark.blob.core.windows.net/graphics/Cog%20Service%20NB/step4.jpg",width:"600"}),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'sentimentTransformer = (\n    TextSentiment()\n    .setLocation(ai_service_location)\n    .setSubscriptionKey(ai_service_key)\n    .setTextCol("text")\n    .setOutputCol("sentiment")\n)\n\n# Extract the sentiment score from the API response body\ngetSentiment = SQLTransformer(\n    statement="SELECT *, sentiment.document.sentiment as sentimentLabel FROM __THIS__"\n)\n')),(0,i.kt)("h3",{id:"tying-it-all-together"},"Tying it all together"),(0,i.kt)("p",null,"Now that we have built the stages of our pipeline it's time to chain them together into a single model that can be used to process batches of incoming data"),(0,i.kt)("img",{src:"https://mmlspark.blob.core.windows.net/graphics/Cog%20Service%20NB/full_pipe_2.jpg",width:"800"}),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.stages import SelectColumns\n\n# Select the final coulmns\ncleanupColumns = SelectColumns().setCols(\n    ["url", "firstCeleb", "text", "sentimentLabel"]\n)\n\ncelebrityQuoteAnalysis = PipelineModel(\n    stages=[\n        bingSearch,\n        getUrls,\n        celebs,\n        firstCeleb,\n        recognizeText,\n        getText,\n        sentimentTransformer,\n        getSentiment,\n        cleanupColumns,\n    ]\n)\n\ncelebrityQuoteAnalysis.transform(bingParameters).show(5)\n')))}m.isMDXComponent=!0}}]);