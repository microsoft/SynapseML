---
title: Quickstart - Apply Phi Model with HuggingFace CausalLM
hide_title: true
status: stable
---
# Apply Phi model with HuggingFace Causal ML

![HuggingFace Logo](https://huggingface.co/front/assets/huggingface_logo-noborder.svg)

**HuggingFace** is a popular open-source platform that develops computation tools for building application using machine learning. It is widely known for its Transformers library which contains open-source implementation of transformer models for text, image, and audio task.

[**Phi 3**](https://azure.microsoft.com/en-us/blog/introducing-phi-3-redefining-whats-possible-with-slms/) is a family of AI models developed by Microsoft, designed to redefine what is possible with small language models (SLMs). Phi-3 models are the most compatable and cost-effective SLMs, [outperforming models of the same size and even larger ones in language](https://news.microsoft.com/source/features/ai/the-phi-3-small-language-models-with-big-potential/?msockid=26355e446adb6dfa06484f956b686c27), reasoning, coding, and math benchmarks. 

![Phi 3 model performance](https://mmlspark.blob.core.windows.net/graphics/The-Phi-3-small-language-models-with-big-potential-1.jpg)

To make it easier to scale up causal language model prediction on a large dataset, we have integrated [HuggingFace Causal LM](https://huggingface.co/docs/transformers/tasks/language_modeling) with SynapseML. This integration makes it easy to use the Apache Spark distributed computing framework to process large data on text generation tasks.

This tutorial shows hot to apply [phi3 model](https://huggingface.co/collections/microsoft/phi-3-6626e15e9585a200d2d761e3) at scale with no extra setting.


```python
# %pip install --upgrade transformers==4.49.0 -q
```


```python
chats = [
    (1, "fix grammar: helol mi friend"),
    (2, "What is HuggingFace"),
    (3, "translate to Spanish: hello"),
]

chat_df = spark.createDataFrame(chats, ["row_index", "content"])
chat_df.show()
```

## Define and Apply Phi3 model

The following example demonstrates how to load the remote Phi 3 model from HuggingFace and apply it to chats.


```python
from synapse.ml.hf import HuggingFaceCausalLM

phi3_transformer = (
    HuggingFaceCausalLM()
    .setModelName("microsoft/Phi-3-mini-4k-instruct")
    .setInputCol("content")
    .setOutputCol("result")
    .setModelParam(max_new_tokens=100)
)
result_df = phi3_transformer.transform(chat_df).collect()
display(result_df)
```

## Apply Chat Template


```python
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, MapType, StringType

reviews = [
    (1, "I like SynapseML"),
    (2, "Contoso is awful"),
]
reviews_df = spark.createDataFrame(reviews, ["row_index", "content"])

PROMPT_1 = f"""You are an AI assistant that identifies the sentiment of a given text. Respond with only the single word “positive” or “negative.”
        """


@udf
def make_template(s: str):
    return [{"role": "system", "content": PROMPT_1}, {"role": "user", "content": s}]


reviews_df = reviews_df.withColumn("messages", make_template("content"))

phi3_transformer = (
    HuggingFaceCausalLM()
    .setModelName("microsoft/Phi-3-mini-4k-instruct")
    .setInputCol("messages")
    .setOutputCol("result")
    .setModelParam(max_new_tokens=10)
)
result_df = phi3_transformer.transform(reviews_df).collect()
display(result_df)
```

## Use local cache

By caching the model, you can reduce initialization time. On Fabric, store the model in a Lakehouse and use setCachePath to load it.


```python
# %%sh
# azcopy copy "https://mmlspark.blob.core.windows.net/huggingface/microsoft/Phi-3-mini-4k-instruct" "/lakehouse/default/Files/microsoft/" --recursive=true
```


```python
# phi3_transformer = (
#     HuggingFaceCausalLM()
#     .setCachePath("/lakehouse/default/Files/microsoft/Phi-3-mini-4k-instruct")
#     .setInputCol("content")
#     .setOutputCol("result")
#     .setModelParam(max_new_tokens=1000)
# )
# result_df = phi3_transformer.transform(chat_df).collect()
# display(result_df)
```

## Utilize GPU

To utilize GPU, passing device_map="cuda", torch_dtype="auto" to modelConfig.


```python
phi3_transformer = (
    HuggingFaceCausalLM()
    .setModelName("microsoft/Phi-3-mini-4k-instruct")
    .setInputCol("content")
    .setOutputCol("result")
    .setModelParam(max_new_tokens=100)
    .setModelConfig(
        device_map="cuda",
        torch_dtype="auto",
    )
)
result_df = phi3_transformer.transform(chat_df).collect()
display(result_df)
```

## Phi 4

To try with the newer version of phi 4 model, simply set the model name to be microsoft/Phi-4-mini-instruct.


```python
phi4_transformer = (
    HuggingFaceCausalLM()
    .setModelName("microsoft/Phi-4-mini-instruct")
    .setInputCol("content")
    .setOutputCol("result")
    .setModelParam(max_new_tokens=100)
    .setModelConfig(
        device_map="auto",
        torch_dtype="auto",
        local_files_only=False,
        trust_remote_code=True,
    )
)
result_df = phi4_transformer.transform(chat_df).collect()
display(result_df)
```
