"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[41821],{3905:(e,t,r)=>{r.d(t,{Zo:()=>i,kt:()=>d});var a=r(67294);function n(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function l(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,a)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?l(Object(r),!0).forEach((function(t){n(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):l(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,a,n=function(e,t){if(null==e)return{};var r,a,n={},l=Object.keys(e);for(a=0;a<l.length;a++)r=l[a],t.indexOf(r)>=0||(n[r]=e[r]);return n}(e,t);if(Object.getOwnPropertySymbols){var l=Object.getOwnPropertySymbols(e);for(a=0;a<l.length;a++)r=l[a],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(n[r]=e[r])}return n}var u=a.createContext({}),c=function(e){var t=a.useContext(u),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},i=function(e){var t=c(e.components);return a.createElement(u.Provider,{value:t},e.children)},m={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},p=a.forwardRef((function(e,t){var r=e.components,n=e.mdxType,l=e.originalType,u=e.parentName,i=s(e,["components","mdxType","originalType","parentName"]),p=c(r),d=n,f=p["".concat(u,".").concat(d)]||p[d]||m[d]||l;return r?a.createElement(f,o(o({ref:t},i),{},{components:r})):a.createElement(f,o({ref:t},i))}));function d(e,t){var r=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var l=r.length,o=new Array(l);o[0]=p;var s={};for(var u in t)hasOwnProperty.call(t,u)&&(s[u]=t[u]);s.originalType=e,s.mdxType="string"==typeof e?e:n,o[1]=s;for(var c=2;c<l;c++)o[c]=r[c];return a.createElement.apply(null,o)}return a.createElement.apply(null,r)}p.displayName="MDXCreateElement"},85162:(e,t,r)=>{r.d(t,{Z:()=>o});var a=r(67294),n=r(86010);const l="tabItem_Ymn6";function o(e){let{children:t,hidden:r,className:o}=e;return a.createElement("div",{role:"tabpanel",className:(0,n.Z)(l,o),hidden:r},t)}},74866:(e,t,r)=>{r.d(t,{Z:()=>T});var a=r(83117),n=r(67294),l=r(86010),o=r(12466),s=r(16550),u=r(91980),c=r(67392),i=r(50012);function m(e){return function(e){var t;return(null==(t=n.Children.map(e,(e=>{if(!e||(0,n.isValidElement)(e)&&function(e){const{props:t}=e;return!!t&&"object"==typeof t&&"value"in t}(e))return e;throw new Error(`Docusaurus error: Bad <Tabs> child <${"string"==typeof e.type?e.type:e.type.name}>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.`)})))?void 0:t.filter(Boolean))??[]}(e).map((e=>{let{props:{value:t,label:r,attributes:a,default:n}}=e;return{value:t,label:r,attributes:a,default:n}}))}function p(e){const{values:t,children:r}=e;return(0,n.useMemo)((()=>{const e=t??m(r);return function(e){const t=(0,c.l)(e,((e,t)=>e.value===t.value));if(t.length>0)throw new Error(`Docusaurus error: Duplicate values "${t.map((e=>e.value)).join(", ")}" found in <Tabs>. Every value needs to be unique.`)}(e),e}),[t,r])}function d(e){let{value:t,tabValues:r}=e;return r.some((e=>e.value===t))}function f(e){let{queryString:t=!1,groupId:r}=e;const a=(0,s.k6)(),l=function(e){let{queryString:t=!1,groupId:r}=e;if("string"==typeof t)return t;if(!1===t)return null;if(!0===t&&!r)throw new Error('Docusaurus error: The <Tabs> component groupId prop is required if queryString=true, because this value is used as the search param name. You can also provide an explicit value such as queryString="my-search-param".');return r??null}({queryString:t,groupId:r});return[(0,u._X)(l),(0,n.useCallback)((e=>{if(!l)return;const t=new URLSearchParams(a.location.search);t.set(l,e),a.replace({...a.location,search:t.toString()})}),[l,a])]}function b(e){const{defaultValue:t,queryString:r=!1,groupId:a}=e,l=p(e),[o,s]=(0,n.useState)((()=>function(e){let{defaultValue:t,tabValues:r}=e;if(0===r.length)throw new Error("Docusaurus error: the <Tabs> component requires at least one <TabItem> children component");if(t){if(!d({value:t,tabValues:r}))throw new Error(`Docusaurus error: The <Tabs> has a defaultValue "${t}" but none of its children has the corresponding value. Available values are: ${r.map((e=>e.value)).join(", ")}. If you intend to show no default tab, use defaultValue={null} instead.`);return t}const a=r.find((e=>e.default))??r[0];if(!a)throw new Error("Unexpected error: 0 tabValues");return a.value}({defaultValue:t,tabValues:l}))),[u,c]=f({queryString:r,groupId:a}),[m,b]=function(e){let{groupId:t}=e;const r=function(e){return e?`docusaurus.tab.${e}`:null}(t),[a,l]=(0,i.Nk)(r);return[a,(0,n.useCallback)((e=>{r&&l.set(e)}),[r,l])]}({groupId:a}),g=(()=>{const e=u??m;return d({value:e,tabValues:l})?e:null})();(0,n.useLayoutEffect)((()=>{g&&s(g)}),[g]);return{selectedValue:o,selectValue:(0,n.useCallback)((e=>{if(!d({value:e,tabValues:l}))throw new Error(`Can't select invalid tab value=${e}`);s(e),c(e),b(e)}),[c,b,l]),tabValues:l}}var g=r(72389);const v="tabList__CuJ",y="tabItem_LNqP";function h(e){let{className:t,block:r,selectedValue:s,selectValue:u,tabValues:c}=e;const i=[],{blockElementScrollPositionUntilNextRender:m}=(0,o.o5)(),p=e=>{const t=e.currentTarget,r=i.indexOf(t),a=c[r].value;a!==s&&(m(t),u(a))},d=e=>{var t;let r=null;switch(e.key){case"Enter":p(e);break;case"ArrowRight":{const t=i.indexOf(e.currentTarget)+1;r=i[t]??i[0];break}case"ArrowLeft":{const t=i.indexOf(e.currentTarget)-1;r=i[t]??i[i.length-1];break}}null==(t=r)||t.focus()};return n.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,l.Z)("tabs",{"tabs--block":r},t)},c.map((e=>{let{value:t,label:r,attributes:o}=e;return n.createElement("li",(0,a.Z)({role:"tab",tabIndex:s===t?0:-1,"aria-selected":s===t,key:t,ref:e=>i.push(e),onKeyDown:d,onClick:p},o,{className:(0,l.Z)("tabs__item",y,null==o?void 0:o.className,{"tabs__item--active":s===t})}),r??t)})))}function k(e){let{lazy:t,children:r,selectedValue:a}=e;const l=(Array.isArray(r)?r:[r]).filter(Boolean);if(t){const e=l.find((e=>e.props.value===a));return e?(0,n.cloneElement)(e,{className:"margin-top--md"}):null}return n.createElement("div",{className:"margin-top--md"},l.map(((e,t)=>(0,n.cloneElement)(e,{key:t,hidden:e.props.value!==a}))))}function w(e){const t=b(e);return n.createElement("div",{className:(0,l.Z)("tabs-container",v)},n.createElement(h,(0,a.Z)({},e,t)),n.createElement(k,(0,a.Z)({},e,t)))}function T(e){const t=(0,g.Z)();return n.createElement(w,(0,a.Z)({key:String(t)},e))}},31989:(e,t,r)=>{r.d(t,{Z:()=>l});var a=r(67294),n=r(52263);const l=function(e){const{className:t,py:r,scala:l,csharp:o,sourceLink:s}=e,u=(0,n.Z)().siteConfig.customFields.version;let c=`https://mmlspark.blob.core.windows.net/docs/${u}/pyspark/${r}`,i=`https://mmlspark.blob.core.windows.net/docs/${u}/scala/${l}`;return a.createElement("table",null,a.createElement("tbody",null,a.createElement("tr",null,a.createElement("td",null,a.createElement("strong",null,"Python API: "),a.createElement("a",{href:c},t)),a.createElement("td",null,a.createElement("strong",null,"Scala API: "),a.createElement("a",{href:i},t)),a.createElement("td",null,a.createElement("strong",null,"Source: "),a.createElement("a",{href:s},t)))))}},39317:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>f,contentTitle:()=>p,default:()=>v,frontMatter:()=>m,metadata:()=>d,toc:()=>b});var a=r(83117),n=(r(67294),r(3905)),l=r(74866),o=r(85162),s=r(31989);const u=[{value:"ImageTransformer",id:"imagetransformer",level:2},{value:"ImageSetAugmenter",id:"imagesetaugmenter",level:2}],c={toc:u};function i(e){let{components:t,...r}=e;return(0,n.kt)("wrapper",(0,a.Z)({},c,r,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("h2",{id:"imagetransformer"},"ImageTransformer"),(0,n.kt)(l.Z,{defaultValue:"py",values:[{label:"Python",value:"py"},{label:"Scala",value:"scala"}],mdxType:"Tabs"},(0,n.kt)(o.Z,{value:"py",mdxType:"TabItem"},(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.opencv import *\nfrom pyspark.sql.types import FloatType\n\n# images = (spark.read.format("image")\n#         .option("dropInvalid", True)\n#         .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/explainers/images/david-lusvardi-dWcUncxocQY-unsplash.jpg"))\n\nit = (ImageTransformer(inputCol="image", outputCol="features")\n    .resize(224, True)\n    .centerCrop(height=224, width=224)\n    .normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225], color_scale_factor = 1/255)\n    .setTensorElementType(FloatType()))\n\n# it.transform(images).show()\n'))),(0,n.kt)(o.Z,{value:"scala",mdxType:"TabItem"},(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-scala"},'import com.microsoft.azure.synapse.ml.opencv._\n\nval images = (spark.read.format("image")\n    .option("dropInvalid", true)\n    .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/explainers/images/david-lusvardi-dWcUncxocQY-unsplash.jpg"))\n\nval it = (new ImageTransformer()\n      .setOutputCol("out")\n      .resize(height = 15, width = 10))\n\nit.transform(images).show()\n')))),(0,n.kt)(s.Z,{className:"ImageTransformer",py:"synapse.ml.opencv.html#module-synapse.ml.opencv.ImageTransformer",scala:"com/microsoft/azure/synapse/ml/opencv/ImageTransformer.html",csharp:"classSynapse_1_1ML_1_1Opencv_1_1ImageTransformer.html",sourceLink:"https://github.com/microsoft/SynapseML/blob/master/opencv/src/main/scala/com/microsoft/azure/synapse/ml/opencv/ImageTransformer.scala",mdxType:"DocTable"}),(0,n.kt)("h2",{id:"imagesetaugmenter"},"ImageSetAugmenter"),(0,n.kt)(l.Z,{defaultValue:"py",values:[{label:"Python",value:"py"},{label:"Scala",value:"scala"}],mdxType:"Tabs"},(0,n.kt)(o.Z,{value:"py",mdxType:"TabItem"},(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.opencv import *\n\n# images = (spark.read.format("image")\n#         .option("dropInvalid", True)\n#         .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/explainers/images/david-lusvardi-dWcUncxocQY-unsplash.jpg"))\n\nisa = (ImageSetAugmenter()\n    .setInputCol("image")\n    .setOutputCol("augmented")\n    .setFlipLeftRight(True)\n    .setFlipUpDown(True))\n\n# it.transform(images).show()\n'))),(0,n.kt)(o.Z,{value:"scala",mdxType:"TabItem"},(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-scala"},'import com.microsoft.azure.synapse.ml.opencv._\n\nval images = (spark.read.format("image")\n    .option("dropInvalid", true)\n    .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/explainers/images/david-lusvardi-dWcUncxocQY-unsplash.jpg"))\n\nval isa = (new ImageSetAugmenter()\n    .setInputCol("image")\n    .setOutputCol("augmented")\n    .setFlipLeftRight(true)\n    .setFlipUpDown(true))\n\nisa.transform(images).show()\n')))),(0,n.kt)(s.Z,{className:"ImageSetAugmenter",py:"synapse.ml.opencv.html#module-synapse.ml.opencv.ImageSetAugmenter",scala:"com/microsoft/azure/synapse/ml/opencv/ImageSetAugmenter.html",csharp:"classSynapse_1_1ML_1_1Opencv_1_1ImageSetAugmenter.html",sourceLink:"https://github.com/microsoft/SynapseML/blob/master/opencv/src/main/scala/com/microsoft/azure/synapse/ml/opencv/ImageSetAugmenter.scala",mdxType:"DocTable"}))}i.isMDXComponent=!0;const m={title:"Transformers - OpenCV",sidebar_label:"OpenCV",hide_title:!0},p="OpenCV",d={unversionedId:"Quick Examples/transformers/transformers_opencv",id:"version-1.0.5/Quick Examples/transformers/transformers_opencv",title:"Transformers - OpenCV",description:"",source:"@site/versioned_docs/version-1.0.5/Quick Examples/transformers/transformers_opencv.md",sourceDirName:"Quick Examples/transformers",slug:"/Quick Examples/transformers/transformers_opencv",permalink:"/SynapseML/docs/1.0.5/Quick Examples/transformers/transformers_opencv",draft:!1,tags:[],version:"1.0.5",frontMatter:{title:"Transformers - OpenCV",sidebar_label:"OpenCV",hide_title:!0}},f={},b=[...u],g={toc:b};function v(e){let{components:t,...r}=e;return(0,n.kt)("wrapper",(0,a.Z)({},g,r,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("h1",{id:"opencv"},"OpenCV"),(0,n.kt)(i,{mdxType:"OpenCV"}))}v.isMDXComponent=!0}}]);