from pyspark.ml import Transformer
from pyspark.ml.param.shared import (
    HasInputCol,
    HasOutputCol,
    Param,
    Params,
    TypeConverters,
)
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from transformers import AutoTokenizer, AutoModelForCausalLM
from pyspark import keyword_only
import re
import os


class _PeekableIterator:
    def __init__(self, iterable):
        self._iterator = iter(iterable)
        self._cache = []

    def __iter__(self):
        return self

    def __next__(self):
        if self._cache:
            return self._cache.pop(0)
        else:
            return next(self._iterator)

    def peek(self, n=1):
        """Peek at the next n elements without consuming them."""
        while len(self._cache) < n:
            try:
                self._cache.append(next(self._iterator))
            except StopIteration:
                break
        if n == 1:
            return self._cache[0] if self._cache else None
        else:
            return self._cache[:n]


class _ModelParam:
    def __init__(self, **kwargs):
        self.param = {}
        self.param.update(kwargs)

    def get_param(self):
        return self.param


class _ModelConfig:
    def __init__(self, **kwargs):
        self.config = {}
        self.config.update(kwargs)

    def get_config(self):
        return self.config

    def set_config(self, **kwargs):
        self.config.update(kwargs)


def camel_to_snake(text):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", text).lower()


class HuggingFaceCausalLM(
    Transformer, HasInputCol, HasOutputCol, DefaultParamsReadable, DefaultParamsWritable
):

    modelName = Param(
        Params._dummy(),
        "modelName",
        "model name",
        typeConverter=TypeConverters.toString,
    )
    inputCol = Param(
        Params._dummy(),
        "inputCol",
        "input column",
        typeConverter=TypeConverters.toString,
    )
    outputCol = Param(
        Params._dummy(),
        "outputCol",
        "output column",
        typeConverter=TypeConverters.toString,
    )
    modelParam = Param(
        Params._dummy(),
        "modelParam",
        "Model Parameters, passed to .generate(). For more details, check https://huggingface.co/docs/transformers/en/main_classes/text_generation#transformers.GenerationConfig",
    )
    modelConfig = Param(
        Params._dummy(),
        "modelConfig",
        "Model configuration, passed to AutoModelForCausalLM.from_pretrained(). For more details, check https://huggingface.co/docs/transformers/en/model_doc/auto#transformers.AutoModelForCausalLM",
    )
    cachePath = Param(
        Params._dummy(),
        "cachePath",
        "cache path for the model. could be a lakehouse path",
        typeConverter=TypeConverters.toString,
    )
    deviceMap = Param(
        Params._dummy(),
        "deviceMap",
        "Specifies a model parameter for the device Map. For GPU usage with models such as Phi 3, set it to 'cuda'.",
        typeConverter=TypeConverters.toString,
    )
    torchDtype = Param(
        Params._dummy(),
        "torchDtype",
        "Specifies a model parameter for the torch dtype. For GPU usage with models such as Phi 3, set it to 'auto'.",
        typeConverter=TypeConverters.toString,
    )

    @keyword_only
    def __init__(
        self,
        modelName=None,
        inputCol=None,
        outputCol=None,
        cachePath=None,
        deviceMap=None,
        torchDtype=None,
    ):
        super(HuggingFaceCausalLM, self).__init__()
        self._setDefault(
            modelName=modelName,
            inputCol=inputCol,
            outputCol=outputCol,
            modelParam=_ModelParam(),
            modelConfig=_ModelConfig(),
            cachePath=None,
            deviceMap=None,
            torchDtype=None,
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setModelName(self, value):
        return self._set(modelName=value)

    def getModelName(self):
        return self.getOrDefault(self.modelName)

    def setInputCol(self, value):
        return self._set(inputCol=value)

    def getInputCol(self):
        return self.getOrDefault(self.inputCol)

    def setOutputCol(self, value):
        return self._set(outputCol=value)

    def getOutputCol(self):
        return self.getOrDefault(self.outputCol)

    def setModelParam(self, **kwargs):
        param = _ModelParam(**kwargs)
        return self._set(modelParam=param)

    def getModelParam(self):
        return self.getOrDefault(self.modelParam)

    def setModelConfig(self, **kwargs):
        config = _ModelConfig(**kwargs)
        return self._set(modelConfig=config)

    def getModelConfig(self):
        return self.getOrDefault(self.modelConfig)

    def setCachePath(self, value):
        return self._set(cachePath=value)

    def getCachePath(self):
        return self.getOrDefault(self.cachePath)

    def setDeviceMap(self, value):
        return self._set(deviceMap=value)

    def getDeviceMap(self):
        return self.getOrDefault(self.deviceMap)

    def setTorchDtype(self, value):
        return self._set(torchDtype=value)

    def getTorchDtype(self):
        return self.getOrDefault(self.torchDtype)

    def load_model(self):
        """
        Loads model and tokenizer either from cache or the HuggingFace Hub
        """
        model_name = self.getModelName()
        model_config = self.getModelConfig().get_config()
        device_map = self.getDeviceMap()
        torch_dtype = self.getTorchDtype()

        if device_map:
            model_config["device_map"] = device_map
        if torch_dtype:
            model_config["torch_dtype"] = torch_dtype

        if self.getCachePath():

            hf_cache = self.getCachePath()
            if not os.path.isdir(hf_cache):
                raise NotADirectoryError(f"Directory does not exist: {hf_cache}")

            model = AutoModelForCausalLM.from_pretrained(
                hf_cache, local_files_only=True, **model_config
            )
            tokenizer = AutoTokenizer.from_pretrained(hf_cache, local_files_only=True)
        else:
            model = AutoModelForCausalLM.from_pretrained(model_name, **model_config)
            tokenizer = AutoTokenizer.from_pretrained(model_name)

        return model, tokenizer

    def _predict_single_complete(self, prompt, model, tokenizer):
        param = self.getModelParam().get_param()
        inputs = tokenizer(prompt, return_tensors="pt").input_ids
        outputs = model.generate(inputs, **param)
        decoded_output = tokenizer.batch_decode(outputs, skip_special_tokens=True)[0]
        return decoded_output

    def _predict_single_chat(self, prompt, model, tokenizer):
        param = self.getModelParam().get_param()
        if isinstance(prompt, list):
            chat = prompt
        else:
            chat = [{"role": "user", "content": prompt}]
        formatted_chat = tokenizer.apply_chat_template(
            chat, tokenize=False, add_generation_prompt=True
        )
        tokenized_chat = tokenizer(
            formatted_chat, return_tensors="pt", add_special_tokens=False
        )
        inputs = {
            key: tensor.to(model.device) for key, tensor in tokenized_chat.items()
        }
        merged_inputs = {**inputs, **param}
        outputs = model.generate(**merged_inputs)
        decoded_output = tokenizer.decode(
            outputs[0][inputs["input_ids"].size(1) :], skip_special_tokens=True
        )
        return decoded_output

    def _process_partition(self, iterator, task):
        """Process each partition of the data."""
        peekable_iterator = _PeekableIterator(iterator)
        try:
            first_row = peekable_iterator.peek()
        except StopIteration:
            return None

        model, tokenizer = self.load_model()

        for row in peekable_iterator:
            prompt = row[self.getInputCol()]
            if task == "chat":
                result = self._predict_single_chat(prompt, model, tokenizer)
            elif task == "complete":
                result = self._predict_single_complete(prompt, model, tokenizer)
            row_dict = row.asDict()
            row_dict[self.getOutputCol()] = result
            yield Row(**row_dict)

    def _transform(self, dataset):
        input_schema = dataset.schema
        output_schema = StructType(
            input_schema.fields + [StructField(self.getOutputCol(), StringType(), True)]
        )
        result_rdd = dataset.rdd.mapPartitions(
            lambda partition: self._process_partition(partition, "chat")
        )
        result_df = result_rdd.toDF(output_schema)
        return result_df

    def complete(self, dataset):
        input_schema = dataset.schema
        output_schema = StructType(
            input_schema.fields + [StructField(self.getOutputCol(), StringType(), True)]
        )
        result_rdd = dataset.rdd.mapPartitions(
            lambda partition: self._process_partition(partition, "complete")
        )
        result_df = result_rdd.toDF(output_schema)
        return result_df
