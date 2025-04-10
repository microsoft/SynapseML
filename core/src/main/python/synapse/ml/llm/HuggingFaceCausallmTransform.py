import os

from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import (
    HasInputCol,
    HasOutputCol,
    Param,
    Params,
    TypeConverters,
)
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from transformers import AutoModelForCausalLM, AutoTokenizer


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


def broadcast_model(cachePath, modelConfig):
    bc_computable = _BroadcastableModel(cachePath, modelConfig)
    sc = SparkSession.builder.getOrCreate().sparkContext
    return sc.broadcast(bc_computable)


class _BroadcastableModel:
    def __init__(self, model_path=None, model_config=None):
        self.model_path = model_path
        self.model = None
        self.tokenizer = None
        self.model_config = model_config

    def load_model(self):
        if self.model_path and os.path.exists(self.model_path):
            model_config = self.model_config.get_config()
            self.model = AutoModelForCausalLM.from_pretrained(
                self.model_path, local_files_only=True, **model_config
            )
            self.tokenizer = AutoTokenizer.from_pretrained(
                self.model_path, local_files_only=True
            )
        else:
            raise ValueError(f"Model path {self.model_path} does not exist.")

    def __getstate__(self):
        return {"model_path": self.model_path, "model_config": self.model_config}

    def __setstate__(self, state):
        self.model_path = state.get("model_path")
        self.model_config = state.get("model_config")
        self.model = None
        self.tokenizer = None
        if self.model_path:
            self.load_model()


class HuggingFaceCausalLM(
    Transformer, HasInputCol, HasOutputCol, DefaultParamsReadable, DefaultParamsWritable
):

    modelName = Param(
        Params._dummy(),
        "modelName",
        "huggingface causal lm model name",
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
    task = Param(
        Params._dummy(),
        "task",
        "Specifies the task, can be chat or completion.",
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
        "cache path for the model. A shared location between the workers, could be a lakehouse path",
        typeConverter=TypeConverters.toString,
    )
    deviceMap = Param(
        Params._dummy(),
        "deviceMap",
        "Specifies a model parameter for the device map. It can also be set with modelParam. Commonly used values include 'auto', 'cuda', or 'cpu'. You may want to check your model documentation for device map",
        typeConverter=TypeConverters.toString,
    )
    torchDtype = Param(
        Params._dummy(),
        "torchDtype",
        "Specifies a model parameter for the torch dtype. It can be set with modelParam. The most commonly used value is 'auto'. You may want to check your model documentation for torch dtype.",
        typeConverter=TypeConverters.toString,
    )

    @keyword_only
    def __init__(
        self,
        modelName=None,
        inputCol=None,
        outputCol=None,
        task="chat",
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
            task=task,
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

    def setTask(self, value):
        supported_values = ["completion", "chat"]
        if value not in supported_values:
            raise ValueError(
                f"Task must be one of {supported_values}, but got '{value}'."
            )
        return self._set(task=value)

    def getTask(self):
        return self.getOrDefault(self.task)

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

    def getBCObject(self):
        return self.bcObject

    def _predict_single_completion(self, prompt, model, tokenizer):
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

    def _process_partition(self, iterator, bc_object):
        """Process each partition of the data."""
        peekable_iterator = _PeekableIterator(iterator)
        try:
            first_row = peekable_iterator.peek()
        except StopIteration:
            return None

        if bc_object:
            lc_object = bc_object.value
            model = lc_object.model
            tokenizer = lc_object.tokenizer
        else:
            model_name = self.getModelName()
            model_config = self.getModelConfig().get_config()
            model = AutoModelForCausalLM.from_pretrained(model_name, **model_config)
            tokenizer = AutoTokenizer.from_pretrained(model_name)

        task = self.getTask() if self.getTask() else "chat"

        for row in peekable_iterator:
            prompt = row[self.getInputCol()]
            if task == "chat":
                result = self._predict_single_chat(prompt, model, tokenizer)
            elif task == "completion":
                result = self._predict_single_completion(prompt, model, tokenizer)
            else:
                raise ValueError(
                    f"Unsupported task '{task}'. Supported tasks are 'chat' and 'completion'."
                )
            row_dict = row.asDict()
            row_dict[self.getOutputCol()] = result
            yield Row(**row_dict)

    def _transform(self, dataset):
        if self.getCachePath():
            bc_object = broadcast_model(self.getCachePath(), self.getModelConfig())
        else:
            bc_object = None
        input_schema = dataset.schema
        output_schema = StructType(
            input_schema.fields + [StructField(self.getOutputCol(), StringType(), True)]
        )
        result_rdd = dataset.rdd.mapPartitions(
            lambda partition: self._process_partition(partition, bc_object)
        )
        result_df = result_rdd.toDF(output_schema)
        return result_df
