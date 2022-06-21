"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[2037],{3905:function(e,t,n){n.d(t,{Zo:function(){return d},kt:function(){return u}});var a=n(7294);function l(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){l(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function r(e,t){if(null==e)return{};var n,a,l=function(e,t){if(null==e)return{};var n,a,l={},i=Object.keys(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||(l[n]=e[n]);return l}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(l[n]=e[n])}return l}var s=a.createContext({}),p=function(e){var t=a.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},d=function(e){var t=p(e.components);return a.createElement(s.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var n=e.components,l=e.mdxType,i=e.originalType,s=e.parentName,d=r(e,["components","mdxType","originalType","parentName"]),m=p(n),u=l,h=m["".concat(s,".").concat(u)]||m[u]||c[u]||i;return n?a.createElement(h,o(o({ref:t},d),{},{components:n})):a.createElement(h,o({ref:t},d))}));function u(e,t){var n=arguments,l=t&&t.mdxType;if("string"==typeof e||l){var i=n.length,o=new Array(i);o[0]=m;var r={};for(var s in t)hasOwnProperty.call(t,s)&&(r[s]=t[s]);r.originalType=e,r.mdxType="string"==typeof e?e:l,o[1]=r;for(var p=2;p<i;p++)o[p]=n[p];return a.createElement.apply(null,o)}return a.createElement.apply(null,n)}m.displayName="MDXCreateElement"},301:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return r},contentTitle:function(){return s},metadata:function(){return p},toc:function(){return d},default:function(){return m}});var a=n(3117),l=n(102),i=(n(7294),n(3905)),o=["components"],r={title:"Build System Commands",hide_title:!0,sidebar_label:"Build System Commands",description:"SynapseML Development Setup"},s="SynapseML Development Setup",p={unversionedId:"reference/developer-readme",id:"reference/developer-readme",title:"Build System Commands",description:"SynapseML Development Setup",source:"@site/docs/reference/developer-readme.md",sourceDirName:"reference",slug:"/reference/developer-readme",permalink:"/SynapseML/docs/next/reference/developer-readme",tags:[],version:"current",frontMatter:{title:"Build System Commands",hide_title:!0,sidebar_label:"Build System Commands",description:"SynapseML Development Setup"},sidebar:"docs",previous:{title:"SynapseML Autologging",permalink:"/SynapseML/docs/next/mlflow/autologging"},next:{title:"Contributing Guide",permalink:"/SynapseML/docs/next/reference/contributing_guide"}},d=[{value:"Scala build commands",id:"scala-build-commands",children:[{value:"<code>compile</code>, <code>test:compile</code> and <code>it:compile</code>",id:"compile-testcompile-and-itcompile",children:[],level:3},{value:"<code>test</code>",id:"test",children:[],level:3},{value:"<code>scalastyle</code>",id:"scalastyle",children:[],level:3},{value:"<code>unidoc</code>",id:"unidoc",children:[],level:3}],level:2},{value:"Python Commands",id:"python-commands",children:[{value:"<code>createCondaEnv</code>",id:"createcondaenv",children:[],level:3},{value:"<code>cleanCondaEnv</code>",id:"cleancondaenv",children:[],level:3},{value:"<code>packagePython</code>",id:"packagepython",children:[],level:3},{value:"<code>generatePythonDoc</code>",id:"generatepythondoc",children:[],level:3},{value:"<code>installPipPackage</code>",id:"installpippackage",children:[],level:3},{value:"<code>testPython</code>",id:"testpython",children:[],level:3}],level:2},{value:"Environment + Publishing Commands",id:"environment--publishing-commands",children:[{value:"<code>getDatasets</code>",id:"getdatasets",children:[],level:3},{value:"<code>setup</code>",id:"setup",children:[],level:3},{value:"<code>package</code>",id:"package",children:[],level:3},{value:"<code>publishBlob</code>",id:"publishblob",children:[],level:3},{value:"<code>publishLocal</code>",id:"publishlocal",children:[],level:3},{value:"<code>publishDocs</code>",id:"publishdocs",children:[],level:3},{value:"<code>publishSigned</code>",id:"publishsigned",children:[],level:3},{value:"<code>sonatypeRelease</code>",id:"sonatyperelease",children:[],level:3}],level:2}],c={toc:d};function m(e){var t=e.components,n=(0,l.Z)(e,o);return(0,i.kt)("wrapper",(0,a.Z)({},c,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"synapseml-development-setup"},"SynapseML Development Setup"),(0,i.kt)("ol",null,(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("a",{parentName:"li",href:"https://www.oracle.com/java/technologies/javase/jdk11-archive-downloads.html"},"Install JDK 11"),(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"You may need an Oracle login to download."))),(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("a",{parentName:"li",href:"https://www.scala-sbt.org/1.x/docs/Setup.html"},"Install SBT")),(0,i.kt)("li",{parentName:"ol"},"Fork the repository on github",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"See how to here: ",(0,i.kt)("a",{parentName:"li",href:"https://docs.github.com/en/get-started/quickstart/fork-a-repo"},"Fork a repo - GitHub Docs")))),(0,i.kt)("li",{parentName:"ol"},"Clone your fork",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"git clone https://github.com/<your GitHub handle>/SynapseML.git")),(0,i.kt)("li",{parentName:"ul"},"This will automatically add your fork as the default remote, called ",(0,i.kt)("inlineCode",{parentName:"li"},"origin")))),(0,i.kt)("li",{parentName:"ol"},"Add another Git Remote to track the original SynapseML repo. It's recommended to call it ",(0,i.kt)("inlineCode",{parentName:"li"},"upstream"),":",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"git remote add upstream https://github.com/microsoft/SynapseML.git")),(0,i.kt)("li",{parentName:"ul"},"See more about Git remotes here: ",(0,i.kt)("a",{parentName:"li",href:"https://git-scm.com/book/en/v2/Git-Basics-Working-with-Remotes"},"Git - Working with remotes")))),(0,i.kt)("li",{parentName:"ol"},"Go to the directory where you cloed the repo (e.g., ",(0,i.kt)("inlineCode",{parentName:"li"},"SynapseML"),") with ",(0,i.kt)("inlineCode",{parentName:"li"},"cd SynapseML")),(0,i.kt)("li",{parentName:"ol"},"Run sbt to compile and grab datasets",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("inlineCode",{parentName:"li"},"sbt setup")))),(0,i.kt)("li",{parentName:"ol"},(0,i.kt)("a",{parentName:"li",href:"https://www.jetbrains.com/idea/download"},"Install IntelliJ")),(0,i.kt)("li",{parentName:"ol"},"Configure IntelliJ",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"Install ",(0,i.kt)("a",{parentName:"li",href:"https://plugins.jetbrains.com/plugin/1347-scala"},"Scala plugin")," during initialization"),(0,i.kt)("li",{parentName:"ul"},(0,i.kt)("strong",{parentName:"li"},"OPEN")," the synapseml directory from IntelliJ"),(0,i.kt)("li",{parentName:"ul"},"If the project does not automatically import,click on ",(0,i.kt)("inlineCode",{parentName:"li"},"build.sbt")," and import project"))),(0,i.kt)("li",{parentName:"ol"},"Prepare your Python Environment",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"Install ",(0,i.kt)("a",{parentName:"li",href:"https://docs.conda.io/en/latest/miniconda.html"},"Miniconda")),(0,i.kt)("li",{parentName:"ul"},"Note: if you want to run conda commands from IntelliJ, you may need to select the option to add conda to PATH during installation."),(0,i.kt)("li",{parentName:"ul"},"Activate the ",(0,i.kt)("inlineCode",{parentName:"li"},"synapseml")," conda environment by running ",(0,i.kt)("inlineCode",{parentName:"li"},"conda env create -f environment.yml")," from the ",(0,i.kt)("inlineCode",{parentName:"li"},"synapseml")," directory."))),(0,i.kt)("li",{parentName:"ol"},"Install pre-commit",(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"This repository uses the ",(0,i.kt)("a",{parentName:"li",href:"https://pre-commit.com/index.html"},"pre-commit")," tool to manage git hooks and enforce linting/coding styles."),(0,i.kt)("li",{parentName:"ul"},"The hooks are configured in ",(0,i.kt)("a",{parentName:"li",href:"https://github.com/microsoft/SynapseML/blob/master/environment.yml"},".pre-commit-config.yaml"),"."),(0,i.kt)("li",{parentName:"ul"},"To use the hooks, please run the following commands:")),(0,i.kt)("pre",{parentName:"li"},(0,i.kt)("code",{parentName:"pre",className:"language-bash"},"pip install pre-commit\npre-commit install\n")),(0,i.kt)("ul",{parentName:"li"},(0,i.kt)("li",{parentName:"ul"},"Now ",(0,i.kt)("inlineCode",{parentName:"li"},"pre-commit")," should automatically run on every ",(0,i.kt)("inlineCode",{parentName:"li"},"git commit")," operation to find AND fix linting issues.")))),(0,i.kt)("blockquote",null,(0,i.kt)("p",{parentName:"blockquote"},"NOTE"),(0,i.kt)("p",{parentName:"blockquote"},"If you will be regularly contributing to the SynapseML repo, you'll want to keep your fork synced with the\nupstream repository. Please read ",(0,i.kt)("a",{parentName:"p",href:"https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/syncing-a-fork"},"this GitHub doc"),"\nto know more and learn techniques about how to do it.")),(0,i.kt)("h1",{id:"publishing-and-using-build-secrets"},"Publishing and Using Build Secrets"),(0,i.kt)("p",null,"To use secrets in the build you must be part of the synapsemlkeyvault\nand azure subscription. If you are MSFT internal would like to be\nadded please reach out ",(0,i.kt)("inlineCode",{parentName:"p"},"synapseml-support@microsoft.com")),(0,i.kt)("h1",{id:"sbt-command-guide"},"SBT Command Guide"),(0,i.kt)("h2",{id:"scala-build-commands"},"Scala build commands"),(0,i.kt)("h3",{id:"compile-testcompile-and-itcompile"},(0,i.kt)("inlineCode",{parentName:"h3"},"compile"),", ",(0,i.kt)("inlineCode",{parentName:"h3"},"test:compile")," and ",(0,i.kt)("inlineCode",{parentName:"h3"},"it:compile")),(0,i.kt)("p",null,"Compiles the main, test, and integration test classes respectively"),(0,i.kt)("h3",{id:"test"},(0,i.kt)("inlineCode",{parentName:"h3"},"test")),(0,i.kt)("p",null,"Runs all synapsemltests"),(0,i.kt)("h3",{id:"scalastyle"},(0,i.kt)("inlineCode",{parentName:"h3"},"scalastyle")),(0,i.kt)("p",null,"Runs scalastyle check"),(0,i.kt)("h3",{id:"unidoc"},(0,i.kt)("inlineCode",{parentName:"h3"},"unidoc")),(0,i.kt)("p",null,"Generates documentation for scala sources"),(0,i.kt)("h2",{id:"python-commands"},"Python Commands"),(0,i.kt)("h3",{id:"createcondaenv"},(0,i.kt)("inlineCode",{parentName:"h3"},"createCondaEnv")),(0,i.kt)("p",null,"Creates a conda environment ",(0,i.kt)("inlineCode",{parentName:"p"},"synapseml")," from ",(0,i.kt)("inlineCode",{parentName:"p"},"environment.yml")," if it does not already exist.\nThis env is used for python testing. ",(0,i.kt)("strong",{parentName:"p"},"Activate this env before using python build commands.")),(0,i.kt)("h3",{id:"cleancondaenv"},(0,i.kt)("inlineCode",{parentName:"h3"},"cleanCondaEnv")),(0,i.kt)("p",null,"Removes ",(0,i.kt)("inlineCode",{parentName:"p"},"synapseml")," conda env"),(0,i.kt)("h3",{id:"packagepython"},(0,i.kt)("inlineCode",{parentName:"h3"},"packagePython")),(0,i.kt)("p",null,"Compiles scala, runs python generation scripts, and creates a wheel"),(0,i.kt)("h3",{id:"generatepythondoc"},(0,i.kt)("inlineCode",{parentName:"h3"},"generatePythonDoc")),(0,i.kt)("p",null,"Generates documentation for generated python code"),(0,i.kt)("h3",{id:"installpippackage"},(0,i.kt)("inlineCode",{parentName:"h3"},"installPipPackage")),(0,i.kt)("p",null,"Installs generated python wheel into existing env"),(0,i.kt)("h3",{id:"testpython"},(0,i.kt)("inlineCode",{parentName:"h3"},"testPython")),(0,i.kt)("p",null,"Generates and runs python tests"),(0,i.kt)("h2",{id:"environment--publishing-commands"},"Environment + Publishing Commands"),(0,i.kt)("h3",{id:"getdatasets"},(0,i.kt)("inlineCode",{parentName:"h3"},"getDatasets")),(0,i.kt)("p",null,"Downloads all datasets used in tests to target folder"),(0,i.kt)("h3",{id:"setup"},(0,i.kt)("inlineCode",{parentName:"h3"},"setup")),(0,i.kt)("p",null,"Combination of ",(0,i.kt)("inlineCode",{parentName:"p"},"compile"),", ",(0,i.kt)("inlineCode",{parentName:"p"},"test:compile"),", ",(0,i.kt)("inlineCode",{parentName:"p"},"it:compile"),", ",(0,i.kt)("inlineCode",{parentName:"p"},"getDatasets")),(0,i.kt)("h3",{id:"package"},(0,i.kt)("inlineCode",{parentName:"h3"},"package")),(0,i.kt)("p",null,"Packages the library into a jar"),(0,i.kt)("h3",{id:"publishblob"},(0,i.kt)("inlineCode",{parentName:"h3"},"publishBlob")),(0,i.kt)("p",null,"Publishes Jar to synapseml's azure blob based maven repo. (Requires Keys)"),(0,i.kt)("h3",{id:"publishlocal"},(0,i.kt)("inlineCode",{parentName:"h3"},"publishLocal")),(0,i.kt)("p",null,"Publishes library to local maven repo"),(0,i.kt)("h3",{id:"publishdocs"},(0,i.kt)("inlineCode",{parentName:"h3"},"publishDocs")),(0,i.kt)("p",null,"Publishes scala and python doc to synapseml's build azure storage account. (Requires Keys)"),(0,i.kt)("h3",{id:"publishsigned"},(0,i.kt)("inlineCode",{parentName:"h3"},"publishSigned")),(0,i.kt)("p",null,"Publishes the library to sonatype staging repo"),(0,i.kt)("h3",{id:"sonatyperelease"},(0,i.kt)("inlineCode",{parentName:"h3"},"sonatypeRelease")),(0,i.kt)("p",null,"Promotes the published sonatype artifact"))}m.isMDXComponent=!0}}]);