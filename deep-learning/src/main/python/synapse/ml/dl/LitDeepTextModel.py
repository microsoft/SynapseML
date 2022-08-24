# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

import inspect

import pytorch_lightning as pl
import torch
import torch.nn.functional as F
import torch.optim as optim
from pytorch_lightning.utilities import _module_available
from transformers import AutoTokenizer, AutoModelForSequenceClassification


class LitDeepTextModel(pl.LightningModule):
    def __init__(
          self,
          checkpoint,
          text_col,
          label_col,
          num_labels,
          additional_layers_to_train,
          optimizer_name,
          loss_name,
      ):
          super(LitDeepTextModel, self).__init__()

          self.checkpoint = checkpoint
          self.text_col = text_col
          self.label_col = label_col
          self.num_labels = num_labels
          self.additional_layers_to_train = additional_layers_to_train
          self.optimizer_name = optimizer_name
          self.loss_name = loss_name
          
          self.tokenizer = AutoTokenizer.from_pretrained(self.checkpoint)

          self._check_params()

          # Freeze those weights
          if "bert" in self.checkpoint:
#             for name, param in self.model.named_parameters():
#                 if 'classifier' not in name:
#                     param.requires_grad = False
            for p in self.model.bert.parameters():
                p.requires_grad = False

          self.save_hyperparameters(
              "checkpoint",
              "text_col",
              "label_col",
              "num_labels",
              "additional_layers_to_train",
              "optimizer_name",
              "loss_name",
          )

    def _check_params(self):
        # HUGGINGFACE.TRANSFORMERS
        try:
            self.model = AutoModelForSequenceClassification.from_pretrained(self.checkpoint, num_labels=self.num_labels)
        except Exception as err:
            raise ValueError(f"No checkpoint {self.checkpoint} found: {err=}, {type(err)=}")

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

#     # tune last additional_layers_to_train layers
#     def _fine_tune_layers(self):
#         children = list(self.model.children())
#         added_layer, cur_layer = 0, -1
#         while added_layer < self.additional_layers_to_train and -cur_layer < len(
#             children
#         ):
#             tunable = False
#             for p in children[cur_layer].parameters():
#                 p.requires_grad = True
#                 tunable = True
#             # only tune those layers contain parameters
#             if tunable:
#                 added_layer += 1
#             cur_layer -= 1

    def forward(self, **inputs):
        return self.model(**inputs)

    def configure_optimizers(self):
#         self._fine_tune_layers()
        params_to_update = filter(lambda p: p.requires_grad, self.model.parameters())
        # note that for different model recommended lr is different
        return self.optimizer_fn(params_to_update, lr=5e-5)

    def training_step(self, batch, batch_idx):
        loss = self._step(batch, False)
        self.log("train_loss", loss)
        print(f"*************batch_id:{batch_idx}, train_loss:{loss}*****************")
        return loss

    def _step(self, batch, validation):
#         labels = batch[self.label_col]
        inputs = {"input_ids": batch["input_ids"],
                  "attention_mask": batch["attention_mask"],
                  "labels": batch["labels"]}
        outputs = self(**inputs)
        logits = outputs.logits
#         predictions = torch.argmax(logits, dim=-1)
        loss = outputs.loss
#         loss = self.loss_fn(logits, label.long())
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
        self.log("test_loss", loss)
        return loss
