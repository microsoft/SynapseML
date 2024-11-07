"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[42073],{3905:(e,i,n)=>{n.d(i,{Zo:()=>c,kt:()=>m});var t=n(67294);function a(e,i,n){return i in e?Object.defineProperty(e,i,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[i]=n,e}function r(e,i){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var t=Object.getOwnPropertySymbols(e);i&&(t=t.filter((function(i){return Object.getOwnPropertyDescriptor(e,i).enumerable}))),n.push.apply(n,t)}return n}function o(e){for(var i=1;i<arguments.length;i++){var n=null!=arguments[i]?arguments[i]:{};i%2?r(Object(n),!0).forEach((function(i){a(e,i,n[i])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):r(Object(n)).forEach((function(i){Object.defineProperty(e,i,Object.getOwnPropertyDescriptor(n,i))}))}return e}function s(e,i){if(null==e)return{};var n,t,a=function(e,i){if(null==e)return{};var n,t,a={},r=Object.keys(e);for(t=0;t<r.length;t++)n=r[t],i.indexOf(n)>=0||(a[n]=e[n]);return a}(e,i);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(t=0;t<r.length;t++)n=r[t],i.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var l=t.createContext({}),p=function(e){var i=t.useContext(l),n=i;return e&&(n="function"==typeof e?e(i):o(o({},i),e)),n},c=function(e){var i=p(e.components);return t.createElement(l.Provider,{value:i},e.children)},d={inlineCode:"code",wrapper:function(e){var i=e.children;return t.createElement(t.Fragment,{},i)}},u=t.forwardRef((function(e,i){var n=e.components,a=e.mdxType,r=e.originalType,l=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),u=p(n),m=a,g=u["".concat(l,".").concat(m)]||u[m]||d[m]||r;return n?t.createElement(g,o(o({ref:i},c),{},{components:n})):t.createElement(g,o({ref:i},c))}));function m(e,i){var n=arguments,a=i&&i.mdxType;if("string"==typeof e||a){var r=n.length,o=new Array(r);o[0]=u;var s={};for(var l in i)hasOwnProperty.call(i,l)&&(s[l]=i[l]);s.originalType=e,s.mdxType="string"==typeof e?e:a,o[1]=s;for(var p=2;p<r;p++)o[p]=n[p];return t.createElement.apply(null,o)}return t.createElement.apply(null,n)}u.displayName="MDXCreateElement"},40573:(e,i,n)=>{n.r(i),n.d(i,{assets:()=>l,contentTitle:()=>o,default:()=>d,frontMatter:()=>r,metadata:()=>s,toc:()=>p});var t=n(83117),a=(n(67294),n(3905));const r={title:"Distributed Training",sidebar_label:"Distributed Training"},o=void 0,s={unversionedId:"Explore Algorithms/Deep Learning/Distributed Training",id:"version-1.0.7/Explore Algorithms/Deep Learning/Distributed Training",title:"Distributed Training",description:"Why Simple Deep Learning",source:"@site/versioned_docs/version-1.0.7/Explore Algorithms/Deep Learning/Distributed Training.md",sourceDirName:"Explore Algorithms/Deep Learning",slug:"/Explore Algorithms/Deep Learning/Distributed Training",permalink:"/SynapseML/docs/1.0.7/Explore Algorithms/Deep Learning/Distributed Training",draft:!1,tags:[],version:"1.0.7",frontMatter:{title:"Distributed Training",sidebar_label:"Distributed Training"},sidebar:"docs",previous:{title:"ONNX",permalink:"/SynapseML/docs/1.0.7/Explore Algorithms/Deep Learning/ONNX"},next:{title:"Quickstart - Fine-tune a Text Classifier",permalink:"/SynapseML/docs/1.0.7/Explore Algorithms/Deep Learning/Quickstart - Fine-tune a Text Classifier"}},l={},p=[{value:"Why Simple Deep Learning",id:"why-simple-deep-learning",level:3},{value:"SynapseML&#39;s Simple DNN",id:"synapsemls-simple-dnn",level:3},{value:"Why Horovod",id:"why-horovod",level:3},{value:"Why Pytorch Lightning",id:"why-pytorch-lightning",level:3},{value:"Sample usage with DeepVisionClassifier",id:"sample-usage-with-deepvisionclassifier",level:3},{value:"Examples",id:"examples",level:2}],c={toc:p};function d(e){let{components:i,...n}=e;return(0,a.kt)("wrapper",(0,t.Z)({},c,n,{components:i,mdxType:"MDXLayout"}),(0,a.kt)("h3",{id:"why-simple-deep-learning"},"Why Simple Deep Learning"),(0,a.kt)("p",null,"Creating a Spark-compatible deep learning system can be challenging for users who may not have a\nthorough understanding of deep learning and distributed systems. Additionally, writing custom deep learning\nscripts may be a cumbersome and time-consuming task.\nSynapseML aims to simplify this process by building on top of the ",(0,a.kt)("a",{parentName:"p",href:"https://github.com/horovod/horovod"},"Horovod")," Estimator, a general-purpose\ndistributed deep learning model that is compatible with SparkML, and ",(0,a.kt)("a",{parentName:"p",href:"https://github.com/Lightning-AI/lightning"},"Pytorch-lightning"),",\na lightweight wrapper around the popular PyTorch deep learning framework."),(0,a.kt)("p",null,"SynapseML's simple deep learning toolkit makes it easy to use modern deep learning methods in Apache Spark.\nBy providing a collection of Estimators, SynapseML enables users to perform distributed transfer learning on\nspark clusters to solve custom machine learning tasks without requiring in-depth domain expertise.\nWhether you're a data scientist, data engineer, or business analyst this project aims to make modern deep-learning methods easy to use for new domain-specific problems."),(0,a.kt)("h3",{id:"synapsemls-simple-dnn"},"SynapseML's Simple DNN"),(0,a.kt)("p",null,"SynapseML goes beyond the limited support for deep networks in SparkML and provides out-of-the-box solutions for various common scenarios:"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Visual Classification: Users can apply transfer learning for image classification tasks, using pretrained models and fine-tuning them to solve custom classification problems."),(0,a.kt)("li",{parentName:"ul"},"Text Classification: SynapseML simplifies the process of implementing natural language processing tasks such as sentiment analysis, text classification, and language modeling by providing prebuilt models and tools."),(0,a.kt)("li",{parentName:"ul"},"And more coming soon")),(0,a.kt)("h3",{id:"why-horovod"},"Why Horovod"),(0,a.kt)("p",null,"Horovod is a distributed deep learning framework developed by Uber, which has become popular for its ability to scale\ndeep learning tasks across multiple GPUs and compute nodes efficiently. It's designed to work with TensorFlow, Keras, PyTorch, and Apache MXNet."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Scalability: Horovod uses efficient communication algorithms like ring-allreduce and hierarchical all reduce, which allow it to scale the training process across multiple GPUs and nodes without significant performance degradation."),(0,a.kt)("li",{parentName:"ul"},"Easy Integration: Horovod can be easily integrated into existing deep learning codebases with minimal changes, making it a popular choice for distributed training."),(0,a.kt)("li",{parentName:"ul"},"Fault Tolerance: Horovod provides fault tolerance features like elastic training. It can dynamically adapt to changes in the number of workers or recover from failures."),(0,a.kt)("li",{parentName:"ul"},"Community Support: Horovod has an active community and is widely used in the industry, which ensures that the framework is continually updated and improved.")),(0,a.kt)("h3",{id:"why-pytorch-lightning"},"Why Pytorch Lightning"),(0,a.kt)("p",null,"PyTorch Lightning is a lightweight wrapper around the popular PyTorch deep learning framework, designed to make it\neasier to write clean, modular, and scalable deep learning code. PyTorch Lightning has several advantages that\nmake it an excellent choice for SynapseML's Simple Deep Learning:"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},"Code Organization: PyTorch Lightning promotes a clean and organized code structure by separating the research code from the engineering code. This property makes it easier to maintain, debug, and share deep learning models."),(0,a.kt)("li",{parentName:"ul"},"Flexibility: PyTorch Lightning retains the flexibility and expressiveness of PyTorch while adding useful abstractions to simplify the training loop and other boilerplate code."),(0,a.kt)("li",{parentName:"ul"},"Built-in Best Practices: PyTorch Lightning incorporates many best practices for deep learning, such as automatic optimization, gradient clipping, and learning rate scheduling, making it easier for users to achieve optimal performance."),(0,a.kt)("li",{parentName:"ul"},"Compatibility: PyTorch Lightning is compatible with a wide range of popular tools and frameworks, including Horovod, which allows users to easily use distributed training capabilities."),(0,a.kt)("li",{parentName:"ul"},"Rapid Development: With PyTorch Lightning, users can quickly experiment with different model architectures and training strategies without worrying about low-level implementation details.")),(0,a.kt)("h3",{id:"sample-usage-with-deepvisionclassifier"},"Sample usage with DeepVisionClassifier"),(0,a.kt)("p",null,"DeepVisionClassifier incorporates all models supported by ",(0,a.kt)("a",{parentName:"p",href:"https://github.com/pytorch/vision"},"torchvision"),". "),(0,a.kt)("admonition",{type:"note"},(0,a.kt)("p",{parentName:"admonition"},"The current version is based on pytorch_lightning v1.5.0 and torchvision v0.12.0")),(0,a.kt)("p",null,"By providing a spark dataframe that contains an 'imageCol' and 'labelCol', you could directly apply 'transform' function\non it with DeepVisionClassifier."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},'train_df = spark.createDataframe([\n    ("PATH_TO_IMAGE_1.jpg", 1),\n    ("PATH_TO_IMAGE_2.jpg", 2)\n], ["image", "label"])\n\ndeep_vision_classifier = DeepVisionClassifier(\n    backbone="resnet50", # Put your backbone here\n    store=store, # Corresponding store\n    callbacks=callbacks, # Optional callbacks\n    num_classes=17,\n    batch_size=16,\n    epochs=epochs,\n    validation=0.1,\n)\n\ndeep_vision_model = deep_vision_classifier.fit(train_df)\n')),(0,a.kt)("p",null,"DeepVisionClassifier does distributed-training on spark with Horovod under the hood, after this fitting process it returns\na DeepVisionModel. With this code you could use the model for inference directly:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-python"},"pred_df = deep_vision_model.transform(test_df)\n")),(0,a.kt)("h2",{id:"examples"},"Examples"),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"../Quickstart%20-%20Fine-tune%20a%20Text%20Classifier"},"Quickstart - Fine-tune a Text Classifier")),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("a",{parentName:"li",href:"../Quickstart%20-%20Fine-tune%20a%20Vision%20Classifier"},"Quickstart - Fine-tune a Vision Classifier"))))}d.isMDXComponent=!0}}]);