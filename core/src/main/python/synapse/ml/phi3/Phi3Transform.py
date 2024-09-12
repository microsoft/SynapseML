from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param, Params, TypeConverters
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from transformers import AutoTokenizer, AutoModelForCausalLM
from pyspark import keyword_only

class Phi3Transform(Transformer, HasInputCol, HasOutputCol, DefaultParamsReadable, DefaultParamsWritable):
    model_name = Param(Params._dummy(), "model_name", "model name", typeConverter=TypeConverters.toString)
    max_new_tokens = Param(Params._dummy(), "max_new_tokens", "maximum new tokens", typeConverter=TypeConverters.toInt)
    temperature = Param(Params._dummy(), "temperature", "generation temperature", typeConverter=TypeConverters.toFloat)
    
    @keyword_only
    def __init__(self, model_name=None, inputCol=None, outputCol=None, max_new_tokens=100, temperature=1.0):
        super(Phi3Transform, self).__init__()
        self._setDefault(model_name=model_name, max_new_tokens=max_new_tokens, temperature=temperature)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)
    
    @keyword_only
    def setParams(self, model_name=None, inputCol=None, outputCol=None, max_new_tokens=None, temperature=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setModelName(self, value):
        return self._set(model_name=value)

    def getModelName(self):
        return self.getOrDefault(self.model_name)
    
    # TODO: Clean Parameters https://huggingface.co/docs/transformers/v4.42.0/en/main_classes/text_generation
    def setMaxNewTokens(self, value):
        return self._set(max_new_tokens=value)

    def getMaxNewTokens(self):
        return self.getOrDefault(self.max_new_tokens)

    def setTemperature(self, value):
        return self._set(temperature=value)

    def getTemperature(self):
        return self.getOrDefault(self.temperature)

    def setInputCol(self, value):
        return self._set(inputCol=value)
    
    def setOutputCol(self, value):
        return self._set(outputCol=value)

    def _predict_single(self, prompt, model, tokenizer):
        chat = [{"role": "user", "content": prompt}]
        formatted_chat = tokenizer.apply_chat_template(chat, tokenize=False, add_generation_prompt=True)
        tokenized_chat = tokenizer(formatted_chat, return_tensors="pt", add_special_tokens=False)
        inputs = {key: tensor.to(model.device) for key, tensor in tokenized_chat.items()}

        max_new_tokens = self.getMaxNewTokens()
        temperature = self.getTemperature()

        outputs = model.generate(**inputs, max_new_tokens=max_new_tokens, temperature=temperature) # TODO: clean parameters
        decoded_output = tokenizer.decode(outputs[0][inputs['input_ids'].size(1):], skip_special_tokens=True)
        return decoded_output

    @property
    def schema(self):
        return StructType([StructField(self.getOutputCol(), StringType(), True)])

    def _process_partition(self, iterator):
        model_name = self.getModelName()
        tokenizer = AutoTokenizer.from_pretrained(model_name)
        model = AutoModelForCausalLM.from_pretrained(model_name, trust_remote_code=True)
        model.eval()
        
        for row in iterator:
            prompt = row[self.getInputCol()]
            result = self._predict_single(prompt, model, tokenizer)
            
            row_dict = row.asDict()
            row_dict[self.getOutputCol()] = result
            yield Row(**row_dict)
    
    def _transform(self, dataset):
        input_schema = dataset.schema
        output_schema = input_schema.add(StructField(self.getOutputCol(), StringType(), True))
        
        result_rdd = dataset.rdd.mapPartitions(self._process_partition)
        result_df = result_rdd.toDF(output_schema)
        
        return result_df
