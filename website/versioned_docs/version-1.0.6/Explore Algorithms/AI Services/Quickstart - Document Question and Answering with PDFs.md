---
title: Quickstart - Document Question and Answering with PDFs
hide_title: true
status: stable
---
# A Guide to Q&A on PDF Documents

## Introduction
In this notebook, we'll demonstrate how to develop a context-aware question answering framework for any form of a document using [OpenAI models](https://azure.microsoft.com/products/ai-services/openai-service), [SynapseML](https://microsoft.github.io/SynapseML/) and [Azure AI Services](https://azure.microsoft.com/products/ai-services/). In this notebook, we assume that PDF documents are the source of data, however, the same framework can be easiy extended to other document formats too.   

We’ll cover the following key steps:

1. Preprocessing PDF Documents: Learn how to load the PDF documents into a Spark DataFrame, read the documents using the [Azure AI Document Intelligence](https://azure.microsoft.com/products/ai-services/ai-document-intelligence) in Azure AI Services, and use SynapseML to split the documents into chunks.
2. Embedding Generation and Storage: Learn how to generate embeddings for the chunks using SynapseML and [Azure OpenAI Services](https://azure.microsoft.com/products/ai-services/openai-service), store the embeddings in a vector store using [Azure Cognitive Search](https://azure.microsoft.com/products/search), and search the vector store to answer the user’s question.
3. Question Answering Pipeline: Learn how to retrieve relevant document based on the user’s question and provide the answer using [Langchain](https://python.langchain.com/en/latest/index.html#).

We start by installing the necessary python libraries.


```python
%pip install openai==0.28.1 langchain==0.0.331
```

### Step 1: Provide the keys for Azure AI Services and Azure OpenAI to authenticate the applications.

To authenticate Azure AI Services and Azure OpenAI applications, you need to provide the respective API keys. Here is an example of how you can provide the keys in Python code. `find_secret()` function uses Azure Keyvault to get the API keys, however you can directly paste your own keys there.


```python
from pyspark.sql import SparkSession
from synapse.ml.core.platform import find_secret

ai_services_key = find_secret(
    secret_name="ai-services-api-key", keyvault="mmlspark-build-keys"
)
ai_services_location = "eastus"

# Fill in the following lines with your Azure service information
aoai_service_name = "synapseml-openai-2"
aoai_endpoint = f"https://{aoai_service_name}.openai.azure.com/"
aoai_key = find_secret(secret_name="openai-api-key-2", keyvault="mmlspark-build-keys")
aoai_deployment_name_embeddings = "text-embedding-ada-002"
aoai_deployment_name_query = "gpt-35-turbo"
aoai_model_name_query = "gpt-35-turbo"

# Azure Cognitive Search
cogsearch_name = "mmlspark-azure-search"
cogsearch_index_name = "examplevectorindex"
cogsearch_api_key = find_secret(
    secret_name="azure-search-key", keyvault="mmlspark-build-keys"
)
```

### Step 2: Load the PDF documents into a Spark DataFrame.

For this tutorial, we will be using NASA's [Earth](https://www.nasa.gov/sites/default/files/atoms/files/earth_book_2019_tagged.pdf) and [Earth at Night](https://www.nasa.gov/sites/default/files/atoms/files/earth_at_night_508.pdf) e-books. To load PDF documents into a Spark DataFrame, you can use the ```spark.read.format("binaryFile")``` method provided by Apache Spark.


```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

document_path = "wasbs://publicwasb@mmlspark.blob.core.windows.net/NASAEarth"  # path to your document
df = spark.read.format("binaryFile").load(document_path).limit(10).cache()
```

This code will read the PDF documents and create a Spark DataFrame named df with the contents of the PDFs. The DataFrame will have a schema that represents the structure of the PDF documents, including their textual content.

Let's take a glimpse at the contents of the e-books we are working with. Below are some screenshots that showcase the essence of the books; as you can see they contain information about the Earth.

<img src="https://mmlspark.blob.core.windows.net/graphics/notebooks/NASAearthbook_screenshot.png" width="500" />
<img src="https://mmlspark.blob.core.windows.net/graphics/notebooks/NASAearthatnight_screenshot.png" width="460" />

##### Display the raw data from the PDF documents


```python
# Show the dataframe without the content
display(df.drop("content"))
```

### Step 3: Read the documents using Azure AI Document Intelligence.

We utilize [SynapseML](https://microsoft.github.io/SynapseML/), an ecosystem of tools designed to enhance the distributed computing framework [Apache Spark](https://github.com/apache/spark). SynapseML introduces advanced networking capabilities to the Spark ecosystem and offers user-friendly SparkML transformers for various [Azure AI Services](https://azure.microsoft.com/products/ai-services).

Additionally, we employ AnalyzeDocument from Azure AI Services to extract the complete document content and present it in the designated columns called "output_content" and "paragraph."


```python
from synapse.ml.services.form import AnalyzeDocument
from pyspark.sql.functions import col

analyze_document = (
    AnalyzeDocument()
    .setPrebuiltModelId("prebuilt-layout")
    .setSubscriptionKey(ai_services_key)
    .setLocation(ai_services_location)
    .setImageBytesCol("content")
    .setOutputCol("result")
    .setPages(
        "1-15"
    )  # Here we are reading the first 15 pages of the documents for demo purposes
)

analyzed_df = (
    analyze_document.transform(df)
    .withColumn("output_content", col("result.analyzeResult.content"))
    .withColumn("paragraphs", col("result.analyzeResult.paragraphs"))
).cache()
```

We can observe the analayzed Spark DataFrame named ```analyzed_df``` using the following code. Note that we drop the "content" column as it is not needed anymore.


```python
analyzed_df = analyzed_df.drop("content")
display(analyzed_df)
```

### Step 4: Split the documents into chunks.

After analyzing the document, we leverage SynapseML’s PageSplitter to divide the documents into smaller sections, which are subsequently stored in the “chunks” column. This allows for more granular representation and processing of the document content.


```python
from synapse.ml.featurize.text import PageSplitter

ps = (
    PageSplitter()
    .setInputCol("output_content")
    .setMaximumPageLength(4000)
    .setMinimumPageLength(3000)
    .setOutputCol("chunks")
)

splitted_df = ps.transform(analyzed_df)
display(splitted_df)
```

Note that the chunks for each document are presented in a single row inside an array. In order to embed all the chunks in the following cells, we need to have each chunk in a separate row. To accomplish that, we first explode these arrays so there is only one chunk in each row, then filter the Spark DataFrame in order to only keep the path to the document and the chunk in a single row.


```python
# Each column contains many chunks for the same document as a vector.
# Explode will distribute and replicate the content of a vecor across multple rows
from pyspark.sql.functions import explode, col

exploded_df = splitted_df.select("path", explode(col("chunks")).alias("chunk")).select(
    "path", "chunk"
)
display(exploded_df)
```

### Step 5: Generate Embeddings.

To produce embeddings for each chunk, we utilize both SynapseML and Azure OpenAI Service. By integrating the Azure OpenAI service with SynapseML, we can leverage the power of the Apache Spark distributed computing framework to process numerous prompts using the OpenAI service. This integration enables the SynapseML embedding client to generate embeddings in a distributed manner, enabling efficient processing of large volumes of data. If you're interested in applying large language models at a distributed scale using Azure OpenAI and Azure Synapse Analytics, you can refer to [this approach](https://microsoft.github.io/SynapseML/docs/Explore%20Algorithms/OpenAI/). For more detailed information on generating embeddings with Azure OpenAI, you can look [here]( https://learn.microsoft.com/azure/cognitive-services/openai/how-to/embeddings?tabs=console).


```python
from synapse.ml.services.openai import OpenAIEmbedding

embedding = (
    OpenAIEmbedding()
    .setSubscriptionKey(aoai_key)
    .setDeploymentName(aoai_deployment_name_embeddings)
    .setCustomServiceName(aoai_service_name)
    .setTextCol("chunk")
    .setErrorCol("error")
    .setOutputCol("embeddings")
)

df_embeddings = embedding.transform(exploded_df)

display(df_embeddings)
```

### Step 6: Store the embeddings in Azure Cognitive Search Vector Store.

[Azure Cognitive Search](https://learn.microsoft.com/azure/search/search-what-is-azure-search) offers a user-friendly interface for creating a vector database, as well as storing and retrieving data using vector search. If you're interested in learning more about vector search, you can look [here](https://github.com/Azure/cognitive-search-vector-pr/tree/main).


Storing data in the AzureCogSearch vector database involves two main steps:

Creating the Index: The first step is to establish the index or schema of the vector database. This entails defining the structure and properties of the data that will be stored and indexed in the vector database.

Adding Chunked Documents and Embeddings: The second step involves adding the chunked documents, along with their corresponding embeddings, to the vector datastore. This allows for efficient storage and retrieval of the data using vector search capabilities.

By following these steps, you can effectively store your chunked documents and their associated embeddings in the AzureCogSearch vector database, enabling seamless retrieval of relevant information through vector search functionality.


```python
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import lit

df_embeddings = (
    df_embeddings.drop("error")
    .withColumn(
        "idx", monotonically_increasing_id().cast("string")
    )  # create index ID for ACS
    .withColumn("searchAction", lit("upload"))
)
```


```python
from synapse.ml.services import writeToAzureSearch
import json

df_embeddings.writeToAzureSearch(
    subscriptionKey=cogsearch_api_key,
    actionCol="searchAction",
    serviceName=cogsearch_name,
    indexName=cogsearch_index_name,
    keyCol="idx",
    vectorCols=json.dumps([{"name": "embeddings", "dimension": 1536}]),
)
```

### Step 7: Ask a Question.

After processing the document, we can proceed to pose a question. We will use [SynapseML](https://microsoft.github.io/SynapseML/docs/Explore%20Algorithms/OpenAI/Quickstart%20-%20OpenAI%20Embedding/) to convert the user's question into an embedding and then utilize cosine similarity to retrieve the top K document chunks that closely match the user's question. It's worth mentioning that alternative similarity metrics can also be employed.


```python
user_question = "What did the astronaut Edgar Mitchell call Earth?"
retrieve_k = 2  # Retrieve the top 2 documents from vector database
```


```python
import requests

# Ask a question and convert to embeddings


def gen_question_embedding(user_question):
    # Convert question to embedding using synapseML
    from synapse.ml.services.openai import OpenAIEmbedding

    df_ques = spark.createDataFrame([(user_question, 1)], ["questions", "dummy"])
    embedding = (
        OpenAIEmbedding()
        .setSubscriptionKey(aoai_key)
        .setDeploymentName(aoai_deployment_name_embeddings)
        .setCustomServiceName(aoai_service_name)
        .setTextCol("questions")
        .setErrorCol("errorQ")
        .setOutputCol("embeddings")
    )
    df_ques_embeddings = embedding.transform(df_ques)
    row = df_ques_embeddings.collect()[0]
    question_embedding = row.embeddings.tolist()
    return question_embedding


def retrieve_k_chunk(k, question_embedding):
    # Retrieve the top K entries
    url = f"https://{cogsearch_name}.search.windows.net/indexes/{cogsearch_index_name}/docs/search?api-version=2023-07-01-Preview"

    payload = json.dumps(
        {"vector": {"value": question_embedding, "fields": "embeddings", "k": k}}
    )
    headers = {
        "Content-Type": "application/json",
        "api-key": cogsearch_api_key,
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    output = json.loads(response.text)
    print(response.status_code)
    return output


# Generate embeddings for the question and retrieve the top k document chunks
question_embedding = gen_question_embedding(user_question)
output = retrieve_k_chunk(retrieve_k, question_embedding)
```

### Step 8: Respond to a User’s Question.

To provide a response to the user's question, we will utilize the [LangChain](https://python.langchain.com/en/latest/index.html) framework. With the LangChain framework we will augment the retrieved documents with respect to the user's question. Following this, we can request a response to the user's question from our framework.


```python
# Import necenssary libraries and setting up OpenAI
from langchain.llms import AzureOpenAI
from langchain import PromptTemplate
from langchain.chains import LLMChain
import openai

openai.api_type = "azure"
openai.api_base = aoai_endpoint
openai.api_version = "2022-12-01"
openai.api_key = aoai_key
```

We can now wrap up the Q&A journey by asking a question and checking the answer. You will see that Edgar Mitchell called Earth "a sparkling blue and white jewel"!


```python
# Define a Question Answering chain function using LangChain
def qa_chain_func():

    # Define llm model
    llm = AzureOpenAI(
        deployment_name=aoai_deployment_name_query,
        model_name=aoai_model_name_query,
        openai_api_key=aoai_key,
        openai_api_version="2022-12-01",
    )

    # Write a preprompt with context and query as variables
    template = """
    context :{context}
    Answer the question based on the context above. If the
    information to answer the question is not present in the given context then reply "I don't know".
    Question: {query}
    Answer: """

    # Define a prompt template
    prompt_template = PromptTemplate(
        input_variables=["context", "query"], template=template
    )
    # Define a chain
    qa_chain = LLMChain(llm=llm, prompt=prompt_template)
    return qa_chain


# Concatenate the content of retrieved documents
context = [i["chunk"] for i in output["value"]]

# Make a Quesion Answer chain function and pass
qa_chain = qa_chain_func()
answer = qa_chain.run({"context": context, "query": user_question})

print(answer)
```
