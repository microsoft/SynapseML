# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import torch
import tensorrt as trt
import model_navigator as nav
from sentence_transformers import SentenceTransformer
from pyspark.ml.functions import predict_batch_udf
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param, Params
from pyspark.sql.types import (
     ArrayType,
     FloatType,
)

class HuggingFaceSentenceEmbedder(Transformer, HasInputCol, HasOutputCol):
    """
    Custom transformer that extends PySpark's Transformer class to
    perform sentence embedding using a model with optional TensorRT acceleration.
    """
    NUM_OPT_ROWS = 100  # Constant for number of rows taken for model optimization
    BATCH_SIZE_DEFAULT = 64    

    # Define additional parameters
    runtime = Param(
        Params._dummy(),
        "runtime",
        "Specifies the runtime environment: cpu, cuda, onnxrt, or tensorrt",
    )
    batchSize = Param(Params._dummy(), "batchSize", "Batch size for embeddings", int)
    modelName = Param(Params._dummy(), "modelName", "Full Model Name parameter")

    def __init__(
        self,
        inputCol=None,
        outputCol=None,
        runtime=None,
        batchSize=None,
        modelName=None,
    ):
        """
        Initialize the HuggingFaceSentenceEmbedder with input/output columns and optional TRT flag.
        """
        super(HuggingFaceSentenceEmbedder, self).__init__()
        self._setDefault(
            runtime="cpu",
            batchSize=self.BATCH_SIZE_DEFAULT,
        )
        self._set(
            inputCol=inputCol,
            outputCol=outputCol,
            runtime=runtime,
            batchSize=batchSize if batchSize is not None else self.BATCH_SIZE_DEFAULT,
            modelName=modelName,
        )
        self.optData = None
        self.model = None        

    # Setter method for batchSize
    def setBatchSize(self, value):
        self._set(batchSize=value)
        return self

    # Getter method for batchSize
    def getBatchSize(self):
        return self.getOrDefault(self.batchSize)

    # Sets the runtime environment for the model.
    # Supported values: 'cpu', 'cuda', 'tensorrt'
    def setRuntime(self, value):
        """
        Sets the runtime environment for the model.
        Supported values: 'cpu', 'cuda', 'tensorrt'
        """
        # if value not in ["cpu", "cuda", "onnxrt", "tensorrt"]:
        if value not in ["cpu", "cuda", "tensorrt"]:
            raise ValueError(
                "Invalid runtime specified. Choose from 'cpu', 'cuda', 'tensorrt'"
            )
        self.setOrDefault(self.runtime, value)

    def getRuntime(self):
        return self.getOrDefault(self.runtime)

    # Setter method for modelName
    def setModelName(self, value):
        self._set(modelName=value)
        return self

    # Getter method for modelName
    def getModelName(self):
        return self.getOrDefault(self.modelName)

    # Optimize the model using Model Navigator with TensorRT configuration.
    def _optimize(self, model):
        conf = nav.OptimizeConfig(
            target_formats=(nav.Format.TENSORRT,),
            runners=("TensorRT",),
            optimization_profile=nav.OptimizationProfile(max_batch_size=64),
            custom_configs=[
                nav.TorchConfig(autocast=True),
                nav.TorchScriptConfig(autocast=True),
                nav.TensorRTConfig(
                    precision=(nav.TensorRTPrecision.FP16,),
                    onnx_parser_flags=[trt.OnnxParserFlag.NATIVE_INSTANCENORM.value],
                ),
            ],
        )

        def _get_dataloader():
            input_data = self.optData
            return [(0, (input_data, {"show_progress_bar": False, "batch_size": self.getBatchSize()}))]
                
        nav.optimize(
            model.encode,
            dataloader=_get_dataloader(), 
            config=conf
        )    

    def _predict_batch_fn(self):
        """
        Create and return a function for batch prediction.
        """              
        runtime = self.getRuntime()
        if self.model == None:
            global model
            modelName = self.getModelName()

            model = SentenceTransformer(modelName, device="cpu" if runtime == "cpu" else "cuda").eval()

            if runtime in ("tensorrt"):
                # this forces navigator to use specific runtime 
                nav.inplace_config.strategy = nav.SelectedRuntimeStrategy("trt-fp16", "TensorRT")

                moduleName = modelName.split("/")[1]
                model = nav.Module(model, name=moduleName, forward_func="forward")
                try:
                    nav.load_optimized()
                except Exception:
                    self._optimize(model)
                    nav.load_optimized()

            self.model = model
 
        def predict(inputs):
            """
            Predict method to encode inputs using the model.
            """
            with torch.no_grad():
                output = model.encode(
                    inputs.tolist(), convert_to_tensor=False, show_progress_bar=False
                )

            return output

        return predict

    # Method to apply the transformation to the dataset
    def _transform(self, dataset, spark):
        """
        Apply the transformation to the input dataset.
        """
        input_col = self.getInputCol()
        output_col = self.getOutputCol()

        df = dataset.take(self.NUM_OPT_ROWS)
        self.optData = [row[input_col] for row in df]

        encode = predict_batch_udf(
            self._predict_batch_fn,
            return_type=ArrayType(FloatType()),
            batch_size=self.getBatchSize(),
        )
        return dataset.withColumn(output_col, encode(input_col))

    def transform(self, dataset, spark=None):
        """
        Public method to transform the dataset.
        """
        return self._transform(dataset, spark)
# result.show()