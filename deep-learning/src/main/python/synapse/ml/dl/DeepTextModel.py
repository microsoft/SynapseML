from horovod.spark.lightning import TorchModel
import numpy as np
import torch
from horovod.spark.lightning import TorchModel
from synapse.ml.dl.PredictionParams import TextPredictionParams
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType
from synapse.ml.dl.utils import keywords_catch
from transformers import AutoTokenizer


class DeepTextModel(TorchModel, TextPredictionParams):
    tokenizer = Param(Params._dummy(), "tokenizer", "tokenizer")

    checkpoint = Param(
        Params._dummy(), "checkpoint", "checkpoint of the deep text classifier"
    )

    max_token_len = Param(Params._dummy(), "max_token_len", "max_token_len")

    @keywords_catch
    def __init__(
        self,
        history=None,
        model=None,
        input_shapes=None,
        optimizer=None,
        run_id=None,
        _metadata=None,
        loss=None,
        loss_constructors=None,
        # diff from horovod
        checkpoint=None,
        tokenizer=None,
        max_token_len=128,
        label_col="label",
        text_col="text",
        prediction_col="prediction",
    ):
        super(DeepTextModel, self).__init__()

        self._setDefault(
            optimizer=None,
            loss=None,
            loss_constructors=None,
            input_shapes=None,
            checkpoint=None,
            max_token_len=128,
            text_col="text",
            label_col="label",
            prediction_col="prediction",
            feature_columns=["text"],
            label_columns=["label"],
            outputCols=["output"],
        )

        kwargs = self._kwargs
        self._set(**kwargs)

    def setTokenizer(self, value):
        return self._set(tokenizer=value)

    def getTokenizer(self):
        return self.getOrDefault(self.tokenizer)

    def setCheckpoint(self, value):
        return self._set(checkpoint=value)

    def getCheckpoint(self):
        return self.getOrDefault(self.checkpoint)

    def setMaxTokenLen(self, value):
        return self._set(max_token_len=value)

    def getMaxTokenLen(self):
        return self.getOrDefault(self.max_token_len)

    def _update_cols(self):
        self.setFeatureColumns([self.getTextCol()])
        self.setLabelColoumns([self.getLabelCol()])

    # override this to encoding text
    def get_prediction_fn(self):
        text_col = self.getTextCol()
        max_token_len = self.getMaxTokenLen()
        tokenizer = self.getTokenizer()

        def predict_fn(model, row):
            text = row[text_col]
            data = tokenizer(
                text,
                max_length=max_token_len,
                padding="max_length",
                truncation=True,
                return_attention_mask=True,
                return_tensors="pt",
            )
            with torch.no_grad():
                outputs = model(**data)
                pred = torch.nn.functional.softmax(outputs.logits, dim=-1)

            return pred

        return predict_fn

    # pytorch_lightning module has its own optimizer configuration
    def getOptimizer(self):
        return None

    def _transform(self, df):
        self._update_cols()
        output_df = super()._transform(df)
        argmax = udf(lambda v: float(np.argmax(v)), returnType=DoubleType())
        pred_df = output_df.withColumn(
            self.getPredictionCol(), argmax(col(self.getOutputCols()[0]))
        )
        return pred_df
