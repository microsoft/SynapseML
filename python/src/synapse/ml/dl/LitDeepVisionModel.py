# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import inspect

import pytorch_lightning as pl
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from pytorch_lightning.utilities import _module_available

_TORCHVISION_AVAILABLE = _module_available("torchvision")
if _TORCHVISION_AVAILABLE:
    import torchvision

    _TORCHVISION_GREATER_EQUAL_0_12_0 = torchvision.__version__ >= "0.12.0"
    if _TORCHVISION_GREATER_EQUAL_0_12_0:
        from torchvision import models
    else:
        raise RuntimeError(
            "torchvision should be >= 0.12.0, found: {}".format(torchvision.__version__)
        )
else:
    raise ModuleNotFoundError("module not found: torchvision")


class LitDeepVisionModel(pl.LightningModule):
    def __init__(
        self,
        backbone,
        num_layers_to_train,
        num_classes,
        input_shapes,
        optimizer_name,
        loss_name,
        label_cols,
        feature_cols,
        loss_weights=[],
        pretrained=True,
        feature_extracting=True,
        dropout_aux=0.7,
    ):
        super(LitDeepVisionModel, self).__init__()

        self.backbone = backbone
        self.num_layers_to_train = num_layers_to_train
        self.num_classes = num_classes
        self.input_shapes = input_shapes
        self.optimizer_name = optimizer_name
        self.loss_name = loss_name
        self.label_cols = label_cols
        self.feature_cols = feature_cols
        self.loss_weights = loss_weights  ## TODO: remove or add support
        self.pretrained = pretrained
        self.feature_extracting = feature_extracting
        self.dropout_aux = dropout_aux

        self._check_params()

        # Freeze those weights
        if feature_extracting:
            for p in self.model.parameters():
                p.requires_grad = False

        # Tune certain layers including num_classes
        if backbone.startswith("alexnet"):
            num_ftrs = self.model.classifier[6].in_features
            self.model.classifier[6] = nn.Linear(num_ftrs, num_classes)
        elif backbone.startswith("densenet"):
            self.model.classifier = nn.Linear(
                self.model.classifier.in_features, num_classes
            )
        elif backbone.startswith("efficientnet"):
            num_ftrs = self.model.classifier[-1].in_features
            self.model.classifier = nn.Linear(num_ftrs, num_classes)
        elif backbone.startswith("googlenet"):
            if self.model.aux_logits:
                self.model.aux1 = models.googlenet.InceptionAux(
                    512, num_classes, dropout=self.dropout_aux
                )
                self.model.aux2 = models.googlenet.InceptionAux(
                    528, num_classes, dropout=self.dropout_aux
                )
            self.model.fc = nn.Linear(self.model.fc.in_features, num_classes)
        elif backbone.startswith("inception"):
            """Inception v3
            Be careful, expects (299,299) sized images and has auxiliary output
            """
            # Handle the auxilary net
            self.model.AuxLogits.fc = nn.Linear(
                self.model.AuxLogits.fc.in_features, num_classes
            )
            # Handle the primary net
            self.model.fc = nn.Linear(self.model.fc.in_features, num_classes)
        elif backbone.startswith("mnasnet"):
            num_ftrs = self.model.classifier[-1].in_features
            self.model.classifier[-1] = nn.Linear(num_ftrs, num_classes)
        elif backbone.startswith("mobilenet"):
            num_ftrs = self.model.classifier[-1].in_features
            self.model.classifier[-1] = nn.Linear(num_ftrs, num_classes)
        elif backbone.startswith("regnet"):
            self.model.fc = nn.Linear(self.model.fc.in_features, num_classes)
        elif (
            backbone.startswith("resnet")
            or backbone.startswith("resnext")
            or backbone.startswith("wide_resnet")
        ):
            self.model.fc = nn.Linear(self.model.fc.in_features, num_classes)
        elif backbone.startswith("shufflenet"):
            self.model.fc = nn.Linear(self.model.fc.in_features, num_classes)
        elif backbone.startswith("squeezenet"):
            self.model.classifier[1] = nn.Conv2d(
                512, num_classes, kernel_size=(1, 1), stride=(1, 1)
            )
        elif backbone.startswith("vgg"):
            num_ftrs = self.model.classifier[6].in_features
            self.model.classifier[6] = nn.Linear(num_ftrs, num_classes)
        elif backbone.startswith("vit"):
            num_ftrs = self.model.heads.head.in_features
            self.model.heads.head = nn.Linear(num_ftrs, num_classes)
        elif backbone.startswith("convnext"):
            num_ftrs = self.model.classifier[-1].in_features
            self.model.classifier[-1] = nn.Linear(num_ftrs, num_classes)

        # The Lightning checkpoint also saves the arguments passed into the LightningModule init
        # under the "hyper_parameters" key in the checkpoint.
        self.save_hyperparameters(
            "backbone",
            "num_layers_to_train",
            "num_classes",
            "input_shapes",
            "optimizer_name",
            "loss_name",
            "label_cols",
            "feature_cols",
            "pretrained",
            "feature_extracting",
            "dropout_aux",
        )

    def _check_params(self):
        # TORCHVISION
        if self.backbone in models.__dict__:
            self.model = models.__dict__[self.backbone](pretrained=self.pretrained)
        # TODO: add HUGGINGFACE.TRANSFORMERS
        else:
            raise ValueError("No model: {} found".format(self.backbone))

        if self.num_layers_to_train < 0 or self.num_layers_to_train > 3:
            raise ValueError(
                "num_layers_to_train has to between 0 and 3: {} found".format(
                    self.num_layers_to_train
                )
            )

        ## TODO: Assign default loss functions for different model type
        if self.loss_name.lower() not in F.__dict__:
            raise ValueError("No loss function: {} found".format(self.loss_name))
        self.loss_fn = F.__dict__[self.loss_name.lower()]

        optimizers_mapping = {
            key.lower(): value
            for key, value in optim.__dict__.items()
            if inspect.isclass(value) and issubclass(value, optim.Optimizer)
        }
        if self.optimizer_name.lower() not in optimizers_mapping:
            raise ValueError("No optimizer: {} found".format(self.optimizer_name))
        self.optimizer_fn = optimizers_mapping[self.optimizer_name.lower()]

    # tune last num_layers_to_train layers
    def _fine_tune_layers(self):
        children = list(self.model.children())
        added_layer, cur_layer = 0, -1
        while added_layer < self.num_layers_to_train and -cur_layer < len(children):
            tunable = False
            for p in children[cur_layer].parameters():
                p.requires_grad = True
                tunable = True
            # only tune those layers contain parameters
            if tunable:
                added_layer += 1
            cur_layer -= 1

    def forward(self, x):
        x = x.float()
        return self.model.forward(x)

    def configure_optimizers(self):
        self._fine_tune_layers()
        params_to_update = filter(lambda p: p.requires_grad, self.model.parameters())
        return self.optimizer_fn(params_to_update)

    def training_step(self, batch, batch_idx):
        loss = self._step(batch, False)
        self.log("train_loss", loss)
        return loss

    def _step(self, batch, validation):
        # Is this the correct assumption? Forward only takes one tensor
        assert len(self.feature_cols) == 1 and len(self.input_shapes) == 1
        inputs = {"x": batch[self.feature_cols[0]].reshape(self.input_shapes[0])}
        labels = [batch[label] for label in self.label_cols]
        outputs = self(**inputs)
        if type(outputs) not in (tuple, list):
            outputs = [outputs]

        if self.backbone.startswith("inception") and not validation:
            # https://discuss.pytorch.org/t/how-to-optimize-inception-model-with-auxiliary-classifiers/7958/9
            losses1 = [
                self.loss_fn(output, label.long())
                for output, label in zip([outputs[0].logits], labels)
            ]
            losses2 = [
                self.loss_fn(output, label.long())
                for output, label in zip([outputs[0].aux_logits], labels)
            ]
            losses = [loss1 + 0.4 * loss2 for loss1, loss2 in zip(losses1, losses2)]
        else:
            losses = [
                self.loss_fn(output, label.long())
                for output, label in zip(outputs, labels)
            ]
        loss = sum(losses)
        return loss

    def validation_step(self, batch, batch_idx):
        loss = self._step(batch, True)
        self.log("val_loss", loss)

    def validation_epoch_end(self, outputs):
        avg_loss = (
            torch.stack([x["val_loss"] for x in outputs]).mean()
            if len(outputs) > 0
            else float("inf")
        )
        self.log("avg_val_loss", avg_loss)

    def test_step(self, batch, batch_idx):
        loss = self._step(batch, False)
        return loss
