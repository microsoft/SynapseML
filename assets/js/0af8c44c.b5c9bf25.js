"use strict";(self.webpackChunksynapseml=self.webpackChunksynapseml||[]).push([[62894],{3905:(e,t,r)=>{r.d(t,{Zo:()=>u,kt:()=>m});var a=r(67294);function n(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function s(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,a)}return r}function o(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?s(Object(r),!0).forEach((function(t){n(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):s(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function i(e,t){if(null==e)return{};var r,a,n=function(e,t){if(null==e)return{};var r,a,n={},s=Object.keys(e);for(a=0;a<s.length;a++)r=s[a],t.indexOf(r)>=0||(n[r]=e[r]);return n}(e,t);if(Object.getOwnPropertySymbols){var s=Object.getOwnPropertySymbols(e);for(a=0;a<s.length;a++)r=s[a],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(n[r]=e[r])}return n}var l=a.createContext({}),c=function(e){var t=a.useContext(l),r=t;return e&&(r="function"==typeof e?e(t):o(o({},t),e)),r},u=function(e){var t=c(e.components);return a.createElement(l.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},d=a.forwardRef((function(e,t){var r=e.components,n=e.mdxType,s=e.originalType,l=e.parentName,u=i(e,["components","mdxType","originalType","parentName"]),d=c(r),m=n,k=d["".concat(l,".").concat(m)]||d[m]||p[m]||s;return r?a.createElement(k,o(o({ref:t},u),{},{components:r})):a.createElement(k,o({ref:t},u))}));function m(e,t){var r=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var s=r.length,o=new Array(s);o[0]=d;var i={};for(var l in t)hasOwnProperty.call(t,l)&&(i[l]=t[l]);i.originalType=e,i.mdxType="string"==typeof e?e:n,o[1]=i;for(var c=2;c<s;c++)o[c]=r[c];return a.createElement.apply(null,o)}return a.createElement.apply(null,r)}d.displayName="MDXCreateElement"},12215:(e,t,r)=>{r.r(t),r.d(t,{assets:()=>l,contentTitle:()=>o,default:()=>p,frontMatter:()=>s,metadata:()=>i,toc:()=>c});var a=r(83117),n=(r(67294),r(3905));const s={title:"Set up Cognitive Services",hide_title:!0,status:"stable"},o="Setting up Azure AI Services and Azure OpenAI resources for SynapseML",i={unversionedId:"Get Started/Set up Cognitive Services",id:"version-1.0.8/Get Started/Set up Cognitive Services",title:"Set up Cognitive Services",description:"In order to use SynapseML's OpenAI or Azure AI Services features, specific Azure resources are required. This documentation walks you through the process of setting up these resources and acquiring the necessary credentials.",source:"@site/versioned_docs/version-1.0.8/Get Started/Set up Cognitive Services.md",sourceDirName:"Get Started",slug:"/Get Started/Set up Cognitive Services",permalink:"/SynapseML/docs/1.0.8/Get Started/Set up Cognitive Services",draft:!1,tags:[],version:"1.0.8",frontMatter:{title:"Set up Cognitive Services",hide_title:!0,status:"stable"},sidebar:"docs",previous:{title:"Install SynapseML",permalink:"/SynapseML/docs/1.0.8/Get Started/Install SynapseML"},next:{title:"Quickstart - Your First Models",permalink:"/SynapseML/docs/1.0.8/Get Started/Quickstart - Your First Models"}},l={},c=[{value:"Azure OpenAI",id:"azure-openai",level:2},{value:"Azure AI Services",id:"azure-ai-services",level:2},{value:"Manage secrets with Azure Key Vault and access the secrets with find_secret",id:"manage-secrets-with-azure-key-vault-and-access-the-secrets-with-find_secret",level:2},{value:"Create Azure Key Vault",id:"create-azure-key-vault",level:3},{value:"Save secret to Azure Key Vault",id:"save-secret-to-azure-key-vault",level:3},{value:"Use find_secret on Microsoft Fabric / Power BI",id:"use-find_secret-on-microsoft-fabric--power-bi",level:3},{value:"Use find_secret on Azure Databricks",id:"use-find_secret-on-azure-databricks",level:3},{value:"Use find_secret on Synapse",id:"use-find_secret-on-synapse",level:3},{value:"Quick Test",id:"quick-test",level:3}],u={toc:c};function p(e){let{components:t,...r}=e;return(0,n.kt)("wrapper",(0,a.Z)({},u,r,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("h1",{id:"setting-up-azure-ai-services-and-azure-openai-resources-for-synapseml"},"Setting up Azure AI Services and Azure OpenAI resources for SynapseML"),(0,n.kt)("p",null,"In order to use SynapseML's OpenAI or Azure AI Services features, specific Azure resources are required. This documentation walks you through the process of setting up these resources and acquiring the necessary credentials."),(0,n.kt)("p",null,"First, create an Azure subscription to create resources."),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"A valid Azure subscription - ",(0,n.kt)("a",{parentName:"li",href:"https://azure.microsoft.com/free/cognitive-services/"},"Create one for free"),".")),(0,n.kt)("h2",{id:"azure-openai"},"Azure OpenAI"),(0,n.kt)("p",null,"The ",(0,n.kt)("a",{parentName:"p",href:"https://azure.microsoft.com/products/cognitive-services/openai-service/"},"Azure OpenAI service")," can be used to solve a large number of natural language tasks through prompting the completion API. To make it easier to scale your prompting workflows from a few examples to large datasets of examples, we have integrated the Azure OpenAI service with the distributed machine learning library SynapseML. This integration makes it easy to use the Apache Spark distributed computing framework to process millions of prompts with the OpenAI service."),(0,n.kt)("p",null,"To set up your Azure OpenAI Resource for SynapseML usage you need to: "),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://aka.ms/oai/access"},"Apply for access to Azure OpenAI")," if you do not already have access. "),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://docs.microsoft.com/azure/cognitive-services/openai/how-to/create-resource?pivots=web-portal#create-a-resource"},"Create an Azure OpenAI resource")," "),(0,n.kt)("li",{parentName:"ul"},"Get your Azure OpenAI resource's key. After your resource is successfully deployed, select ",(0,n.kt)("strong",{parentName:"li"},"Next Steps")," > ",(0,n.kt)("strong",{parentName:"li"},"Go to resource"),". Once at the resource, you can get the key from ",(0,n.kt)("strong",{parentName:"li"},"Resource Management")," > ",(0,n.kt)("strong",{parentName:"li"},"Keys and Endpoint"),". Copy the key and paste it into the notebook. Store keys securely and do not share them. ")),(0,n.kt)("h2",{id:"azure-ai-services"},"Azure AI Services"),(0,n.kt)("p",null,"To set up ",(0,n.kt)("a",{parentName:"p",href:"https://azure.microsoft.com/en-us/products/ai-services"},"Azure AI Services")," for use with SynapseML you first need to:"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://learn.microsoft.com/azure/role-based-access-control/role-assignments-steps"},"Assign yourself the Azure AI Services Contributor role")," to agree to the responsible AI terms and create a resource. "),(0,n.kt)("li",{parentName:"ul"},(0,n.kt)("a",{parentName:"li",href:"https://ms.portal.azure.com/#create/Microsoft.CognitiveServicesAllInOne"},"Create Azure AI service (Decision, Language, Speech, Vision) resource"),". You can follow the steps at ",(0,n.kt)("a",{parentName:"li",href:"https://learn.microsoft.com/en-us/azure/ai-services/multi-service-resource?tabs=windows&pivots=azportal#create-a-new-azure-cognitive-services-resource"},"Create a multi-service resource for Azure AI services"),". "),(0,n.kt)("li",{parentName:"ul"},"Get your Azure AI Services resource's key. After your resource is successfully deployed, select ",(0,n.kt)("strong",{parentName:"li"},"Next Steps")," > ",(0,n.kt)("strong",{parentName:"li"},"Go to resource"),". Once at the resource, you can get the key from ",(0,n.kt)("strong",{parentName:"li"},"Resource Management")," > ",(0,n.kt)("strong",{parentName:"li"},"Keys and Endpoint"),". Copy the key and paste it into the notebook. Store keys securely and do not share them. ")),(0,n.kt)("h2",{id:"manage-secrets-with-azure-key-vault-and-access-the-secrets-with-find_secret"},"Manage secrets with Azure Key Vault and access the secrets with find_secret"),(0,n.kt)("p",null,"After you create an Azure AI resource, you will obtain a resource key. You can use this resource key directly in our notebooks as a string, but we recommend to manage secrets with Azure Key Vault. Azure Key Vault is a cloud-based service that allows you to store and manage cryptographic keys, certificates, and secrets used by cloud applications and services."),(0,n.kt)("p",null,"You can skip the following content if you want to use a secret string instead of Azure Key Vault. This is not recommended for production workloads."),(0,n.kt)("h3",{id:"create-azure-key-vault"},"Create Azure Key Vault"),(0,n.kt)("p",null,"Refer to ",(0,n.kt)("a",{parentName:"p",href:"https://learn.microsoft.com/azure/key-vault/general/quick-create-portal"},"this article")," to create a key vault using Azure Portal"),(0,n.kt)("h3",{id:"save-secret-to-azure-key-vault"},"Save secret to Azure Key Vault"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Go to Access control (IAM) and assign ",(0,n.kt)("strong",{parentName:"li"},"Key Vault Administrator Role")," to yourself."),(0,n.kt)("li",{parentName:"ul"},"Go to Secrets and choose ",(0,n.kt)("strong",{parentName:"li"},"+ Generate/Import"),", create a key with the secret value obtained from Azure AI service."),(0,n.kt)("li",{parentName:"ul"},"Choose ",(0,n.kt)("strong",{parentName:"li"},"Create"),".")),(0,n.kt)("h3",{id:"use-find_secret-on-microsoft-fabric--power-bi"},"Use find_secret on Microsoft Fabric / Power BI"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Go to Azure Key Vault, Select Access control (IAM), Select ",(0,n.kt)("strong",{parentName:"li"},"+ Add"),", Add role assignment."),(0,n.kt)("li",{parentName:"ul"},"Granted the Fabric or Power BI Service Account Get permissions in the Azure Key Vault.")),(0,n.kt)("h3",{id:"use-find_secret-on-azure-databricks"},"Use find_secret on Azure Databricks"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"In the Azure Portal, find and select the Azure Key Vault Instance. Select the ",(0,n.kt)("strong",{parentName:"li"},"Access configuration")," tab under Settings. Set Permission model to Vault access policy."),(0,n.kt)("li",{parentName:"ul"},"On Databricks, go to ",(0,n.kt)("inlineCode",{parentName:"li"},"https://<databricks-instance>#secrets/createScope"),". This URL is case sensitive.")),(0,n.kt)("img",{src:"https://mmlspark.blob.core.windows.net/graphics/adb_create_secret_scope.png",width:"600"}),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Enter the name of the secret scope and choose desired Manage Principal."),(0,n.kt)("li",{parentName:"ul"},"Go to your Azure Key Vault -> Properties and find the ",(0,n.kt)("strong",{parentName:"li"},"DNS Name")," (Vault URI on Azure Key Vault) and ",(0,n.kt)("strong",{parentName:"li"},"Resource ID"),". Enter the DNS Name and Resource ID on Databricks createScope page.")),(0,n.kt)("img",{src:"https://mmlspark.blob.core.windows.net/graphics/adb_find_resource_id.png",width:"600"}),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Click the ",(0,n.kt)("strong",{parentName:"li"},"Create")," Button.")),(0,n.kt)("p",null,"Refer to ",(0,n.kt)("a",{parentName:"p",href:"https://learn.microsoft.com/azure/databricks/security/secrets/secret-scopes#--create-an-azure-key-vault-backed-secret-scope"},"this article")," for more details."),(0,n.kt)("h3",{id:"use-find_secret-on-synapse"},"Use find_secret on Synapse"),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Open the Synapse Studio and go to the Manage tab."),(0,n.kt)("li",{parentName:"ul"},"Under External connections, select Linked services."),(0,n.kt)("li",{parentName:"ul"},"To add a linked service, select New."),(0,n.kt)("li",{parentName:"ul"},"Select the Azure Key Vault tile from the list and select Continue."),(0,n.kt)("li",{parentName:"ul"},"Enter a linked service name and choose the key vault you want to connect to."),(0,n.kt)("li",{parentName:"ul"},"Select Create"),(0,n.kt)("li",{parentName:"ul"},"Click Publish")),(0,n.kt)("ul",null,(0,n.kt)("li",{parentName:"ul"},"Go to Azure Key Vault, Select Access control (IAM), Select ",(0,n.kt)("strong",{parentName:"li"},"+ Add"),", Add role assignment."),(0,n.kt)("li",{parentName:"ul"},"Choose ",(0,n.kt)("strong",{parentName:"li"},"Key Vault Administrator")," in Role blade, select Next."),(0,n.kt)("li",{parentName:"ul"},"In Members blade, choose Assign access to ",(0,n.kt)("strong",{parentName:"li"},"Managed identity"),". Select members, choose the subscription your Synapse Workspace in. For Managed identity, select Synapse workspace, choose your workspace."),(0,n.kt)("li",{parentName:"ul"},"Select ",(0,n.kt)("strong",{parentName:"li"},"Review + assign"),".")),(0,n.kt)("h3",{id:"quick-test"},"Quick Test"),(0,n.kt)("pre",null,(0,n.kt)("code",{parentName:"pre",className:"language-python"},'from synapse.ml.services.language import AnalyzeText\nfrom synapse.ml.core.platform import find_secret\n\nai_service_key = find_secret(\n    secret_name="ai-services-api-key", keyvault="mmlspark-build-keys"\n)  # use your own key vault name and api name\nai_service_location = "eastus"  # use your own AI service location\n\ndf = spark.createDataFrame(\n    data=[\n        ["en", "Dr. Smith has a very modern medical office, and she has great staff."],\n        ["en", "I had a wonderful trip to Seattle last week."],\n    ],\n    schema=["language", "text"],\n)\n\nentity_recognition = (\n    AnalyzeText()\n    .setKind("EntityRecognition")\n    .setLocation(ai_service_location)\n    .setSubscriptionKey(ai_service_key)\n    .setTextCol("text")\n    .setOutputCol("entities")\n    .setErrorCol("error")\n    .setLanguageCol("language")\n)\n\ndf_results = entity_recognition.transform(df)\ndisplay(df_results.select("language", "text", "entities.documents.entities"))\n')),(0,n.kt)("p",null,"Your result should looks like"),(0,n.kt)("table",null,(0,n.kt)("thead",{parentName:"table"},(0,n.kt)("tr",{parentName:"thead"},(0,n.kt)("th",{parentName:"tr",align:null},"language"),(0,n.kt)("th",{parentName:"tr",align:null},"text"),(0,n.kt)("th",{parentName:"tr",align:null},"entities"))),(0,n.kt)("tbody",{parentName:"table"},(0,n.kt)("tr",{parentName:"tbody"},(0,n.kt)("td",{parentName:"tr",align:null},"en"),(0,n.kt)("td",{parentName:"tr",align:null},"Dr. Smith has a very modern medical office, and she has great staff."),(0,n.kt)("td",{parentName:"tr",align:null},'[{"category": "Person", "confidenceScore": 0.98, "length": 5, "offset": 4, "subcategory": null, "text": "Smith"}, {"category": "Location", "confidenceScore": 0.79, "length": 14, "offset": 28, "subcategory": "Structural", "text": "medical office"}, {"category": "PersonType", "confidenceScore": 0.85, "length": 5, "offset": 62, "subcategory": null, "text": "staff"}]')),(0,n.kt)("tr",{parentName:"tbody"},(0,n.kt)("td",{parentName:"tr",align:null},"en"),(0,n.kt)("td",{parentName:"tr",align:null},"I had a wonderful trip to Seattle last week."),(0,n.kt)("td",{parentName:"tr",align:null},'[{"category": "Event", "confidenceScore": 0.74, "length": 4, "offset": 18, "subcategory": null, "text": "trip"}, {"category": "Location", "confidenceScore": 1, "length": 7, "offset": 26, "subcategory": "GPE", "text": "Seattle"}, {"category": "DateTime", "confidenceScore": 0.8, "length": 9, "offset": 34, "subcategory": "DateRange", "text": "last week"}]')))))}p.isMDXComponent=!0}}]);