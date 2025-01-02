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
import subprocess
import shutil
import sys

# from pyspark.ml import Transformer
# from pyspark.ml.param.shared import HasInputCol, HasOutputCol
# from pyspark.sql.types import StructType, StructField, StringType
# from pyspark.sql import Row
# from transformers import AutoTokenizer, AutoModelForCausalLM
# from functools import lru_cache
class Peekable:
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


class ModelParam:
    def __init__(self, **kwargs):
        self.param = {}
        self.param.update(kwargs)

    def get_param(self):
        return self.param


class ModelConfig:
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
    modelParam = Param(Params._dummy(), "modelParam", "Model Parameters")
    modelConfig = Param(Params._dummy(), "modelConfig", "Model configuration")
    useFabricLakehouse = Param(
        Params._dummy(),
        "useFabricLakehouse",
        "Use FabricLakehouse",
        typeConverter=TypeConverters.toBoolean,
    )
    lakehousePath = Param(
        Params._dummy(),
        "lakehousePath",
        "Fabric Lakehouse Path for Model",
        typeConverter=TypeConverters.toString,
    )
    deviceMap = Param(
        Params._dummy(),
        "deviceMap",
        "device map",
        typeConverter=TypeConverters.toString,
    )
    torchDtype = Param(
        Params._dummy(),
        "torchDtype",
        "torch dtype",
        typeConverter=TypeConverters.toString,
    )

    @keyword_only
    def __init__(
        self,
        modelName=None,
        inputCol=None,
        outputCol=None,
        useFabricLakehouse=False,
        lakehousePath=None,
        deviceMap=None,
        torchDtype=None,
    ):
        super(HuggingFaceCausalLM, self).__init__()
        self._setDefault(
            modelName=modelName,
            inputCol=inputCol,
            outputCol=outputCol,
            modelParam=ModelParam(),
            modelConfig=ModelConfig(),
            useFabricLakehouse=useFabricLakehouse,
            lakehousePath=None,
            deviceMap=None,
            torchDtype=None,
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def load_model(self):
        """
        Loads model and tokenizer either from Fabric Lakehouse or the HuggingFace Hub,
        depending on the 'useFabricLakehouse' param.
        """
        model_name = self.getModelName()
        model_config = self.getModelConfig().get_config()
        device_map = self.getDeviceMap()
        torch_dtype = self.getTorchDtype()

        if device_map:
            model_config["device_map"] = device_map
        if torch_dtype:
            model_config["torch_dtype"] = torch_dtype

        if self.getUseFabricLakehouse():
            local_path = (
                self.getLakehousePath() or f"/lakehouse/default/Files/{model_name}"
            )
            model = AutoModelForCausalLM.from_pretrained(
                local_path, local_files_only=True, **model_config
            )
            tokenizer = AutoTokenizer.from_pretrained(local_path, local_files_only=True)
        else:
            model = AutoModelForCausalLM.from_pretrained(model_name, **model_config)
            tokenizer = AutoTokenizer.from_pretrained(model_name)

        return model, tokenizer

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
        param = ModelParam(**kwargs)
        return self._set(modelParam=param)

    def getModelParam(self):
        return self.getOrDefault(self.modelParam)

    def setModelConfig(self, **kwargs):
        config = ModelConfig(**kwargs)
        return self._set(modelConfig=config)

    def getModelConfig(self):
        return self.getOrDefault(self.modelConfig)

    def setLakehousePath(self, value):
        return self._set(lakehousePath=value)

    def getLakehousePath(self):
        return self.getOrDefault(self.lakehousePath)

    def setUseFabricLakehouse(self, value: bool):
        return self._set(useFabricLakehouse=value)

    def getUseFabricLakehouse(self):
        return self.getOrDefault(self.useFabricLakehouse)

    def setDeviceMap(self, value):
        return self._set(deviceMap=value)

    def getDeviceMap(self):
        return self.getOrDefault(self.deviceMap)

    def setTorchDtype(self, value):
        return self._set(torchDtype=value)

    def getTorchDtype(self):
        return self.getOrDefault(self.torchDtype)

    def _predict_single_complete(self, prompt, model, tokenizer):
        param = self.getModelParam().get_param()
        inputs = tokenizer(prompt, return_tensors="pt").input_ids
        outputs = model.generate(inputs, **param)
        decoded_output = tokenizer.batch_decode(outputs, skip_special_tokens=True)[0]
        return decoded_output

    def _predict_single_chat(self, prompt, model, tokenizer):
        param = self.getModelParam().get_param()
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

    def _transform(self, dataset):
        """Transform method to apply the chat model."""

        def _process_partition(iterator, task):
            """Process each partition of the data."""
            peekable_iterator = Peekable(iterator)
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

        input_schema = dataset.schema
        output_schema = StructType(
            input_schema.fields + [StructField(self.getOutputCol(), StringType(), True)]
        )
        result_rdd = dataset.rdd.mapPartitions(
            lambda partition: _process_partition(partition, "chat")
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


class Downloader:
    def __init__(
        self,
        base_cache_dir="./cache",
        base_url="https://mmlspark.blob.core.windows.net/huggingface/",
    ):
        self.base_cache_dir = base_cache_dir
        self.base_url = base_url

    def _ensure_directory_exists(self, directory_path):
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

    def download_model_from_az(self, repo_id, local_path=None, overwrite="false"):
        local_path = os.path.join(
            local_path or self.base_cache_dir, repo_id.rsplit("/", 1)[0]
        )

        blob_url = f"{self.base_url}{repo_id}"

        self._ensure_directory_exists(local_path)

        command = [
            "azcopy",
            "copy",
            blob_url,
            local_path,
            "--recursive=true",
            f"--overwrite={overwrite}",
        ]

        try:
            with subprocess.Popen(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            ) as proc:
                for line in proc.stdout:
                    print(line, end="")
                for line in proc.stderr:
                    print(line, end="", file=sys.stderr)
                return_code = proc.wait()
                if return_code != 0:
                    raise subprocess.CalledProcessError(return_code, command)
        except subprocess.CalledProcessError as e:
            raise IOError("Error during download ", e.stderr)

    def copy_contents_to_lakehouse(
        self, dst, src=None, lakehouse_path=None, dirs_exist_ok=False
    ):
        if not any(os.scandir("/lakehouse/")):
            raise FileNotFoundError("No lakehouse attached")

        if lakehouse_path is None:
            lakehouse_path = f"/lakehouse/default/Files/{dst}"
        os.makedirs(lakehouse_path, exist_ok=True)

        src = os.path.join(src or self.base_cache_dir, dst)

        for item in os.listdir(src):
            src_path = os.path.join(src, item)
            dst_path = os.path.join(lakehouse_path, item)

            if os.path.isdir(src_path):
                shutil.copytree(src_path, dst_path, dirs_exist_ok=dirs_exist_ok)
            else:
                shutil.copy2(src_path, dst_path)
