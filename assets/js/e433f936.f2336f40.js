"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[2496],{3905:(e,t,a)=>{a.d(t,{Zo:()=>c,kt:()=>m});var n=a(67294);function r(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function o(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function i(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?o(Object(a),!0).forEach((function(t){r(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):o(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function l(e,t){if(null==e)return{};var a,n,r=function(e,t){if(null==e)return{};var a,n,r={},o=Object.keys(e);for(n=0;n<o.length;n++)a=o[n],t.indexOf(a)>=0||(r[a]=e[a]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)a=o[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(r[a]=e[a])}return r}var s=n.createContext({}),p=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):i(i({},t),e)),a},c=function(e){var t=p(e.components);return n.createElement(s.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},g=n.forwardRef((function(e,t){var a=e.components,r=e.mdxType,o=e.originalType,s=e.parentName,c=l(e,["components","mdxType","originalType","parentName"]),g=p(a),m=r,h=g["".concat(s,".").concat(m)]||g[m]||u[m]||o;return a?n.createElement(h,i(i({ref:t},c),{},{components:a})):n.createElement(h,i({ref:t},c))}));function m(e,t){var a=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=a.length,i=new Array(o);i[0]=g;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l.mdxType="string"==typeof e?e:r,i[1]=l;for(var p=2;p<o;p++)i[p]=a[p];return n.createElement.apply(null,i)}return n.createElement.apply(null,a)}g.displayName="MDXCreateElement"},13506:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>s,contentTitle:()=>i,default:()=>u,frontMatter:()=>o,metadata:()=>l,toc:()=>p});var n=a(83117),r=(a(67294),a(3905));const o={title:"Quickstart - Apply Phi Model with HuggingFace CausalLM",hide_title:!0,status:"stable"},i="Apply Phi model with HuggingFace Causal ML",l={unversionedId:"Explore Algorithms/Deep Learning/Quickstart - Apply Phi Model with HuggingFace CausalLM",id:"version-v1.0.11/Explore Algorithms/Deep Learning/Quickstart - Apply Phi Model with HuggingFace CausalLM",title:"Quickstart - Apply Phi Model with HuggingFace CausalLM",description:"HuggingFace Logo",source:"@site/versioned_docs/version-v1.0.11/Explore Algorithms/Deep Learning/Quickstart - Apply Phi Model with HuggingFace CausalLM.md",sourceDirName:"Explore Algorithms/Deep Learning",slug:"/Explore Algorithms/Deep Learning/Quickstart - Apply Phi Model with HuggingFace CausalLM",permalink:"/SynapseML/docs/Explore Algorithms/Deep Learning/Quickstart - Apply Phi Model with HuggingFace CausalLM",draft:!1,tags:[],version:"v1.0.11",frontMatter:{title:"Quickstart - Apply Phi Model with HuggingFace CausalLM",hide_title:!0,status:"stable"}},s={},p=[{value:"Define and Apply Phi3 model",id:"define-and-apply-phi3-model",level:2},{value:"Use local cache",id:"use-local-cache",level:2},{value:"Utilize GPU",id:"utilize-gpu",level:2},{value:"Phi 4",id:"phi-4",level:2}],c={toc:p};function u(e){let{components:t,...a}=e;return(0,r.kt)("wrapper",(0,n.Z)({},c,a,{components:t,mdxType:"MDXLayout"}),(0,r.kt)("h1",{id:"apply-phi-model-with-huggingface-causal-ml"},"Apply Phi model with HuggingFace Causal ML"),(0,r.kt)("p",null,(0,r.kt)("img",{parentName:"p",src:"https://huggingface.co/front/assets/huggingface_logo-noborder.svg",alt:"HuggingFace Logo"})),(0,r.kt)("p",null,(0,r.kt)("strong",{parentName:"p"},"HuggingFace")," is a popular open-source platform that develops computation tools for building application using machine learning. It is widely known for its Transformers library which contains open-source implementation of transformer models for text, image, and audio task."),(0,r.kt)("p",null,(0,r.kt)("a",{parentName:"p",href:"https://azure.microsoft.com/en-us/blog/introducing-phi-3-redefining-whats-possible-with-slms/"},(0,r.kt)("strong",{parentName:"a"},"Phi 3"))," is a family of AI models developed by Microsoft, designed to redefine what is possible with small language models (SLMs). Phi-3 models are the most compatable and cost-effective SLMs, ",(0,r.kt)("a",{parentName:"p",href:"https://news.microsoft.com/source/features/ai/the-phi-3-small-language-models-with-big-potential/?msockid=26355e446adb6dfa06484f956b686c27"},"outperforming models of the same size and even larger ones in language"),", reasoning, coding, and math benchmarks. "),(0,r.kt)("p",null,(0,r.kt)("img",{parentName:"p",src:"https://mmlspark.blob.core.windows.net/graphics/The-Phi-3-small-language-models-with-big-potential-1.jpg",alt:"Phi 3 model performance"})),(0,r.kt)("p",null,"To make it easier to scale up causal language model prediction on a large dataset, we have integrated ",(0,r.kt)("a",{parentName:"p",href:"https://huggingface.co/docs/transformers/tasks/language_modeling"},"HuggingFace Causal LM")," with SynapseML. This integration makes it easy to use the Apache Spark distributed computing framework to process large data on text generation tasks."),(0,r.kt)("p",null,"This tutorial shows hot to apply ",(0,r.kt)("a",{parentName:"p",href:"https://huggingface.co/collections/microsoft/phi-3-6626e15e9585a200d2d761e3"},"phi3 model")," at scale with no extra setting."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},"# %pip install --upgrade transformers==4.49.0 -q\n")),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'chats = [\n    (1, "fix grammar: helol mi friend"),\n    (2, "What is HuggingFace"),\n    (3, "translate to Spanish: hello"),\n]\n\nchat_df = spark.createDataFrame(chats, ["row_index", "content"])\nchat_df.show()\n')),(0,r.kt)("h2",{id:"define-and-apply-phi3-model"},"Define and Apply Phi3 model"),(0,r.kt)("p",null,"The following example demonstrates how to load the remote Phi 3 model from HuggingFace and apply it to chats."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.llm.HuggingFaceCausallmTransform import HuggingFaceCausalLM\n\nphi3_transformer = (\n    HuggingFaceCausalLM()\n    .setModelName("microsoft/Phi-3-mini-4k-instruct")\n    .setInputCol("content")\n    .setOutputCol("result")\n    .setModelParam(max_new_tokens=100)\n)\nresult_df = phi3_transformer.transform(chat_df).collect()\ndisplay(result_df)\n')),(0,r.kt)("h2",{id:"use-local-cache"},"Use local cache"),(0,r.kt)("p",null,"By caching the model, you can reduce initialization time. On Fabric, store the model in a Lakehouse and use setCachePath to load it."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'# %%sh\n# azcopy copy "https://mmlspark.blob.core.windows.net/huggingface/microsoft/Phi-3-mini-4k-instruct" "/lakehouse/default/Files/microsoft/" --recursive=true\n')),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'# phi3_transformer = (\n#     HuggingFaceCausalLM()\n#     .setCachePath("/lakehouse/default/Files/microsoft/Phi-3-mini-4k-instruct")\n#     .setInputCol("content")\n#     .setOutputCol("result")\n#     .setModelParam(max_new_tokens=1000)\n# )\n# result_df = phi3_transformer.transform(chat_df).collect()\n# display(result_df)\n')),(0,r.kt)("h2",{id:"utilize-gpu"},"Utilize GPU"),(0,r.kt)("p",null,'To utilize GPU, passing device_map="cuda", torch_dtype="auto" to modelConfig.'),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'phi3_transformer = (\n    HuggingFaceCausalLM()\n    .setModelName("microsoft/Phi-3-mini-4k-instruct")\n    .setInputCol("content")\n    .setOutputCol("result")\n    .setModelParam(max_new_tokens=100)\n    .setModelConfig(\n        device_map="cuda",\n        torch_dtype="auto",\n    )\n)\nresult_df = phi3_transformer.transform(chat_df).collect()\ndisplay(result_df)\n')),(0,r.kt)("h2",{id:"phi-4"},"Phi 4"),(0,r.kt)("p",null,"To try with the newer version of phi 4 model, simply set the model name to be microsoft/Phi-4-mini-instruct."),(0,r.kt)("pre",null,(0,r.kt)("code",{parentName:"pre",className:"language-python"},'phi4_transformer = (\n    HuggingFaceCausalLM()\n    .setModelName("microsoft/Phi-4-mini-instruct")\n    .setInputCol("content")\n    .setOutputCol("result")\n    .setModelParam(max_new_tokens=100)\n    .setModelConfig(\n        device_map="auto",\n        torch_dtype="auto",\n        local_files_only=False,\n        trust_remote_code=True,\n    )\n)\nresult_df = phi4_transformer.transform(chat_df).collect()\ndisplay(result_df)\n')))}u.isMDXComponent=!0}}]);