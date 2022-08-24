from horovod.spark.lightning import TorchEstimator
from horovod.spark.common.backend import SparkBackend
from pyspark.context import SparkContext

import sys

from horovod.spark.common.backend import SparkBackend
from horovod.spark.lightning import TorchEstimator
import torch
from pyspark.context import SparkContext
from pyspark.ml.param.shared import Param, Params
from pytorch_lightning.utilities import _module_available
from synapse.ml.dl.DeepTextModel import DeepTextModel
from synapse.ml.dl.LitDeepTextModel import LitDeepTextModel
from synapse.ml.dl.utils import keywords_catch
from synapse.ml.dl.PredictionParams import TextPredictionParams
from transformers import AutoTokenizer


class DeepTextClassifier(TorchEstimator, TextPredictionParams):

    checkpoint = Param(
        Params._dummy(), "checkpoint", "checkpoint of the deep text classifier"
    )

    additional_layers_to_train = Param(
        Params._dummy(),
        "additional_layers_to_train",
        "number of last layers to fine tune for the model, should be between 0 and 3",
    )

    num_classes = Param(Params._dummy(), "num_classes", "number of target classes")

    loss_name = Param(
        Params._dummy(),
        "loss_name",
        "string representation of torch.nn.functional loss function for the underlying pytorch_lightning model, e.g. binary_cross_entropy",
    )

    optimizer_name = Param(
        Params._dummy(),
        "optimizer_name",
        "string representation of optimizer function for the underlying pytorch_lightning model",
    )
    
    max_token_len = Param(Params._dummy(), "max_token_len", "max_token_len")

    @keywords_catch
    def __init__(
        self,
        checkpoint=None,
        additional_layers_to_train=0,
        num_classes=None,
        optimizer_name="adam",
        loss_name="cross_entropy",
        max_token_len=100,
        # Classifier args
        label_col="label",
        text_col="text",
        prediction_col="prediction",
        # TorchEstimator args
        num_proc=None,
        backend=None,
        store=None,
        metrics=None,
        loss_weights=None,
        sample_weight_col=None,
        gradient_compression=None,
        input_shapes=None,
        validation=None,
        callbacks=None,
        batch_size=None,
        val_batch_size=None,
        epochs=None,
        verbose=1,
        random_seed=None,
        shuffle_buffer_size=None,
        partitions_per_process=None,
        run_id=None,
        train_minibatch_fn=None,
        train_steps_per_epoch=None,
        validation_steps_per_epoch=None,
        transformation_fn=None,
        transformation_edit_fields=None,
        transformation_removed_fields=None,
        train_reader_num_workers=None,
        trainer_args=None,
        val_reader_num_workers=None,
        reader_pool_type=None,
        label_shapes=None,
        inmemory_cache_all=False,
        num_gpus=None,
        logger=None,
        log_every_n_steps=50,
        data_module=None,
        loader_num_epochs=None,
        terminate_on_nan=False,
        profiler=None,
        debug_data_loader=False,
        train_async_data_loader_queue_size=None,
        val_async_data_loader_queue_size=None,
        use_gpu=True,
        mp_start_method=None,
    ):
        super(DeepTextClassifier, self).__init__()

        self._setDefault(
            checkpoint=None,
            additional_layers_to_train=0,
            num_classes=None,
            optimizer_name="adam",
            loss_name="cross_entropy",
            max_token_len=100,
            feature_cols=["text"],
            label_cols=["label"],
            label_col="label",
            text_col="text",
            prediction_col="prediction",
        )

        kwargs = self._kwargs
        self._set(**kwargs)

        self._update_cols()
        self._update_transformation_fn()

        model = LitDeepTextModel(
            checkpoint=self.getCheckpoint(),
            additional_layers_to_train=self.getAdditionalLayersToTrain(),
            num_labels=self.getNumClasses(),
            optimizer_name=self.getOptimizerName(),
            loss_name=self.getLossName(),
            label_col=self.getLabelCol(),
            text_col=self.getTextCol(),
        )
        self._set(model=model)

    def setCheckpoint(self, value):
        return self._set(checkpoint=value)

    def getCheckpoint(self):
        return self.getOrDefault(self.checkpoint)

    def setAdditionalLayersToTrain(self, value):
        return self._set(additional_layers_to_train=value)

    def getAdditionalLayersToTrain(self):
        return self.getOrDefault(self.additional_layers_to_train)

    def setNumClasses(self, value):
        return self._set(num_classes=value)

    def getNumClasses(self):
        return self.getOrDefault(self.num_classes)

    def setLossName(self, value):
        return self._set(loss_name=value)

    def getLossName(self):
        return self.getOrDefault(self.loss_name)

    def setOptimizerName(self, value):
        return self._set(optimizer_name=value)

    def getOptimizerName(self):
        return self.getOrDefault(self.optimizer_name)

    def setMaxTokenLen(self, value):
        return self._set(max_token_len=value)

    def getMaxTokenLen(self):
        return self.getOrDefault(self.max_token_len)
    

    def _update_cols(self):
        self.setFeatureCols([self.getTextCol()])
        self.setLabelCols([self.getLabelCol()])

    def _fit(self, dataset):
        return super()._fit(dataset)

    # override this method to provide a correct default backend
    def _get_or_create_backend(self):
        backend = self.getBackend()
        num_proc = self.getNumProc()
        if backend is None:
            if num_proc is None:
                num_proc = self._find_num_proc()
            backend = SparkBackend(
                num_proc,
                stdout=sys.stdout,
                stderr=sys.stderr,
                prefix_output_with_timestamp=True,
                verbose=self.getVerbose(),
            )
        elif num_proc is not None:
            raise ValueError(
                'At most one of parameters "backend" and "num_proc" may be specified'
            )
        return backend

    def _find_num_proc(self):
        if self.getUseGpu():
            # set it as number of executors for now (ignoring num_gpus per executor)
            sc = SparkContext.getOrCreate()
            return sc._jsc.sc().getExecutorMemoryStatus().size() - 1
        return None

    def _update_transformation_fn(self):
        
        text_col = self.getTextCol()
        label_col = self.getLabelCol()
        max_token_len = self.getMaxTokenLen()
        # load it inside to avoid `Already borrowed` error (https://github.com/huggingface/tokenizers/issues/537)
        tokenizer = AutoTokenizer.from_pretrained(self.getCheckpoint())

        def _encoding_text(row):
            text = row[text_col]
            label = row[label_col]
            encoding = tokenizer(text,
                                 return_token_type_ids=False,
                                 max_length=max_token_len,
                                 padding="max_length",
                                 truncation=True,
                                 return_attention_mask=True,
                                 return_tensors="pt")
            input_ids = encoding["input_ids"].flatten().numpy()
            attention_mask = encoding["attention_mask"].flatten().numpy()
            return {"input_ids": input_ids,
                    "attention_mask": attention_mask,
                    "labels": torch.tensor(label, dtype=int)}

        self.setTransformationFn(_encoding_text)

    def get_model_class(self):
        return DeepTextModel

    def _get_model_kwargs(self, model, history, optimizer, run_id, metadata):
        return dict(
            history=history,
            model=model,
            optimizer=optimizer,
            input_shapes=self.getInputShapes(),
            run_id=run_id,
            _metadata=metadata,
            loss=self.getLoss(),
            loss_constructors=self.getLossConstructors(),
            checkpoint=self.getCheckpoint(),
            max_token_len=self.getMaxTokenLen(),
            label_col=self.getLabelCol(),
            text_col=self.getTextCol(),
            prediction_col=self.getPredictionCol(),
        )
