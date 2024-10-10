"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[91773],{3905:(e,t,n)=>{n.d(t,{Zo:()=>c,kt:()=>m});var r=n(67294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function a(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?a(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):a(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,r,o=function(e,t){if(null==e)return{};var n,r,o={},a=Object.keys(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);for(r=0;r<a.length;r++)n=a[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var s=r.createContext({}),p=function(e){var t=r.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},c=function(e){var t=p(e.components);return r.createElement(s.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},u=r.forwardRef((function(e,t){var n=e.components,o=e.mdxType,a=e.originalType,s=e.parentName,c=l(e,["components","mdxType","originalType","parentName"]),u=p(n),m=o,g=u["".concat(s,".").concat(m)]||u[m]||d[m]||a;return n?r.createElement(g,i(i({ref:t},c),{},{components:n})):r.createElement(g,i({ref:t},c))}));function m(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var a=n.length,i=new Array(a);i[0]=u;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l.mdxType="string"==typeof e?e:o,i[1]=l;for(var p=2;p<a;p++)i[p]=n[p];return r.createElement.apply(null,i)}return r.createElement.apply(null,n)}u.displayName="MDXCreateElement"},1811:(e,t,n)=>{n.r(t),n.d(t,{assets:()=>s,contentTitle:()=>i,default:()=>d,frontMatter:()=>a,metadata:()=>l,toc:()=>p});var r=n(83117),o=(n(67294),n(3905));const a={title:"Getting Started",sidebar_label:"Getting Started"},i=void 0,l={unversionedId:"Explore Algorithms/Deep Learning/Getting Started",id:"version-1.0.5/Explore Algorithms/Deep Learning/Getting Started",title:"Getting Started",description:"This is a sample with databricks 10.4.x-gpu-ml-scala2.12 runtime",source:"@site/versioned_docs/version-1.0.5/Explore Algorithms/Deep Learning/Getting Started.md",sourceDirName:"Explore Algorithms/Deep Learning",slug:"/Explore Algorithms/Deep Learning/Getting Started",permalink:"/SynapseML/docs/1.0.5/Explore Algorithms/Deep Learning/Getting Started",draft:!1,tags:[],version:"1.0.5",frontMatter:{title:"Getting Started",sidebar_label:"Getting Started"},sidebar:"docs",previous:{title:"Quickstart - Understand and Search Forms",permalink:"/SynapseML/docs/1.0.5/Explore Algorithms/OpenAI/Quickstart - Understand and Search Forms"},next:{title:"ONNX",permalink:"/SynapseML/docs/1.0.5/Explore Algorithms/Deep Learning/ONNX"}},s={},p=[{value:"1. Reinstall horovod using our prepared script",id:"1-reinstall-horovod-using-our-prepared-script",level:2},{value:"2. Install SynapseML Deep Learning Component",id:"2-install-synapseml-deep-learning-component",level:2},{value:"3. Try our sample notebook",id:"3-try-our-sample-notebook",level:2}],c={toc:p};function d(e){let{components:t,...n}=e;return(0,o.kt)("wrapper",(0,r.Z)({},c,n,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("admonition",{type:"note"},(0,o.kt)("p",{parentName:"admonition"},"This is a sample with databricks 10.4.x-gpu-ml-scala2.12 runtime")),(0,o.kt)("h2",{id:"1-reinstall-horovod-using-our-prepared-script"},"1. Reinstall horovod using our prepared script"),(0,o.kt)("p",null,"We build on top of torchvision, horovod and pytorch_lightning, so we need to reinstall horovod by building on specific versions of those packages.\nDownload our ",(0,o.kt)("a",{parentName:"p",href:"https://mmlspark.blob.core.windows.net/publicwasb/horovod_installation.sh"},"horovod installation script")," and upload\nit to databricks dbfs."),(0,o.kt)("p",null,"Add the path of this script to ",(0,o.kt)("inlineCode",{parentName:"p"},"Init Scripts")," section when configuring the spark cluster.\nRestarting the cluster automatically installs horovod v0.25.0 with pytorch_lightning v1.5.0 and torchvision v0.12.0."),(0,o.kt)("h2",{id:"2-install-synapseml-deep-learning-component"},"2. Install SynapseML Deep Learning Component"),(0,o.kt)("p",null,"You could install the single synapseml-deep-learning wheel package to get the full functionality of deep vision classification.\nRun the following command:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-powershell"},"pip install synapseml==1.0.5\n")),(0,o.kt)("p",null,"An alternative is installing the SynapseML jar package in library management section, by adding:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"Coordinate: com.microsoft.azure:synapseml_2.12:1.0.5\nRepository: https://mmlspark.azureedge.net/maven\n")),(0,o.kt)("admonition",{type:"note"},(0,o.kt)("p",{parentName:"admonition"},"If you install the jar package, follow the first two cells of this ",(0,o.kt)("a",{parentName:"p",href:"../Quickstart%20-%20Fine-tune%20a%20Vision%20Classifier#environment-setup----reinstall-horovod-based-on-new-version-of-pytorch"},"sample"),"\nto ensure horovod recognizes SynapseML.")),(0,o.kt)("h2",{id:"3-try-our-sample-notebook"},"3. Try our sample notebook"),(0,o.kt)("p",null,"You could follow the rest of this ","[sample]","(../Quickstart%20-%20Fine-Tune a Vision Classifier) and have a try on your own dataset."),(0,o.kt)("p",null,"Supported models (",(0,o.kt)("inlineCode",{parentName:"p"},"backbone")," parameter for ",(0,o.kt)("inlineCode",{parentName:"p"},"DeepVisionClassifer"),") should be string format of ",(0,o.kt)("a",{parentName:"p",href:"https://github.com/pytorch/vision/blob/v0.12.0/torchvision/models/__init__.py"},"Torchvision-supported models"),";\nYou could also check by running ",(0,o.kt)("inlineCode",{parentName:"p"},"backbone in torchvision.models.__dict__"),"."))}d.isMDXComponent=!0}}]);