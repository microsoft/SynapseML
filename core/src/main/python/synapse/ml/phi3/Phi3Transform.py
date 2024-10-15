from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param, Params, TypeConverters
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from transformers import AutoTokenizer, AutoModelForCausalLM
from pyspark import keyword_only
import re

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
        self.param = {
        }
        self.param.update(kwargs)

    def get_param(self):
        return self.param

class ModelConfig:
    def __init__(self, **kwargs):
        self.config = {
        }
        self.config.update(kwargs)

    def get_config(self):
        return self.config

def camel_to_snake(text):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', text).lower()

class HuggingFaceCausalLM(Transformer, HasInputCol, HasOutputCol, DefaultParamsReadable, DefaultParamsWritable):
    modelName = Param(Params._dummy(), "modelName", "model name", typeConverter=TypeConverters.toString)
    inputCol = Param(Params._dummy(), "inputCol", "input column", typeConverter=TypeConverters.toString)
    outputCol = Param(Params._dummy(), "outputCol", "output column", typeConverter=TypeConverters.toString)
    modelParam = Param(Params._dummy(), "modelParam", "Model Parameters")
    modelConfig = Param(Params._dummy(), "modelConfig", "Model configuration")
    @keyword_only
    def __init__(self, 
                 modelName=None, 
                 inputCol=None, 
                 outputCol=None,

):
        super(HuggingFaceCausalLM, self).__init__()
        self._setDefault(
            modelName=modelName,
            inputCol=inputCol,
            outputCol=outputCol,
            modelParam=ModelParam(),
            modelConfig=ModelConfig()
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
        param = ModelParam(**kwargs)
        return self._set(modelParam=param)

    def getModelParam(self):
        return self.getOrDefault(self.modelParam)

    def setModelConfig(self, **kwargs):
        config = ModelConfig(**kwargs)
        return self._set(modelConfig=config)

    def getModelConfig(self):
        return self.getOrDefault(self.modelConfig)
    
    def _predict_single_complete(self, prompt, model, tokenizer):
        param = self.getModelParam().get_param()
        inputs = tokenizer(prompt, return_tensors="pt").input_ids
        outputs = model.generate(inputs, **param)
        decoded_output = tokenizer.batch_decode(outputs, skip_special_tokens=True)[0]
        return decoded_output

    def _predict_single_chat(self, prompt, model, tokenizer):
        param = self.getModelParam().get_param()
        chat = [{"role": "user", "content": prompt}]
        formatted_chat = tokenizer.apply_chat_template(chat, tokenize=False, add_generation_prompt=True)
        tokenized_chat = tokenizer(formatted_chat, return_tensors="pt", add_special_tokens=False)
        inputs = {key: tensor.to(model.device) for key, tensor in tokenized_chat.items()}
        merged_inputs = {**inputs, **param}
        outputs = model.generate(**merged_inputs)
        decoded_output = tokenizer.decode(outputs[0][inputs['input_ids'].size(1):], skip_special_tokens=True)
        return decoded_output

    @property
    def schema(self):
        return StructType([StructField(self.getOutputCol(), StringType(), True)])

    def _process_partition(self, iterator, task):
        peekable_iterator = Peekable(iterator)
        try:
            first_row = peekable_iterator.peek()
        except StopIteration:
            return
        model_name = self.getModelName()
        tokenizer = AutoTokenizer.from_pretrained(model_name)
        model_config = self.getModelConfig().get_config()
        model = AutoModelForCausalLM.from_pretrained(model_name, **model_config)
        model.eval()
        
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
        output_schema = StructType(input_schema.fields + [StructField(self.getOutputCol(), StringType(), True)])
        result_rdd = dataset.rdd.mapPartitions(lambda partition: self._process_partition(partition, "chat"))
        result_df = result_rdd.toDF(output_schema)
        return result_df
    
    def complete(self, dataset):
        input_schema = dataset.schema
        output_schema = StructType(input_schema.fields + [StructField(self.getOutputCol(), StringType(), True)])
        result_rdd = dataset.rdd.mapPartitions(lambda partition: self._process_partition(partition, "complete"))
        result_df = result_rdd.toDF(output_schema)
        return result_df
