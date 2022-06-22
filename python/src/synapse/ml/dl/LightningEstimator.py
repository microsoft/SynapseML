# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

from pyspark.ml import Estimator
import horovod.spark.lightning as hvd
from synapse.ml.dl.LightningParams import LightningParams


class LightningEstimator(Estimator, LightningParams):
    def __init__(self):
        super(LightningEstimator, self).__init__()
        self._setDefault(
            num_proc=None,
            backend=None,
            store=None,
            loss=None,
            metrics=None,
            loss_weights=None,
            sample_weight_col=None,
            feature_cols=None,
            input_shapes=None,
            validation=None,
            label_cols=None,
            callbacks=None,
            batch_size=None,
            val_batch_size=None,
            epochs=None,
            verbose=1,
            shuffle_buffer_size=None,
            partitions_per_process=10,
            run_id=None,
            train_minibatch_fn=None,
            train_steps_per_epoch=None,
            validation_steps_per_epoch=None,
            transformation_fn=None,
            train_reader_num_workers=None,
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
        )

    def _fit(self, dataset):
        torch_estimator = hvd.TorchEstimator(
            num_proc=self.getNumProc(),
            model=self.model,
            backend=self.getBackend(),
            store=self.getStore(),
            loss=self.getLoss(),
            metrics=self.getMetrics(),
            loss_weights=self.getLossWeights(),
            sample_weight_col=self.getSampleWeightCol(),
            feature_cols=self.getFeatureCols(),
            input_shapes=self.getInputShapes(),
            validation=self.getValidation(),
            label_cols=self.getLabelCols(),
            callbacks=self.getCallbacks(),
            batch_size=self.getBatchSize(),
            val_batch_size=self.getValBatchSize(),
            epochs=self.getEpochs(),
            verbose=self.getVerbose(),
            shuffle_buffer_size=self.getShufflingBufferSize(),
            partitions_per_process=self.getPartitionsPerProcess(),
            run_id=self.getRunId(),
            train_minibatch_fn=self.getTrainMinibatchFn(),
            train_steps_per_epoch=self.getTrainStepsPerEpoch(),
            validation_steps_per_epoch=self.getValidationStepsPerEpoch(),
            transformation_fn=self.getTransformationFn(),
            train_reader_num_workers=self.getTrainReaderNumWorker(),
            val_reader_num_workers=self.getValReaderNumWorker,
            reader_pool_type=self.getReaderPoolType(),
            label_shapes=self.getLabelShapes(),
            inmemory_cache_all=self.getInMemoryCacheAll(),
            num_gpus=self.getNumGPUs(),
            logger=self.getLogger(),
            log_every_n_steps=self.getLogEveryNSteps(),
            data_module=self.getDataModule(),
            loader_num_epochs=self.getLoaderNumEpochs(),
            terminate_on_nan=self.getTerminateOnNan(),
            profiler=self.getProfiler(),
        )
        torch_model = torch_estimator.fit(dataset)
        return torch_model
