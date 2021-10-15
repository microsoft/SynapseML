# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

DEFAULT_URL = "https://mmlspark.azureedge.net/datasets/CNTKModels/"

#' A class for downloading CNTK pretrained models in R. To download all models use the downloadModels
#' function. To browse models from the microsoft server please use remoteModels.
#'
#' Creates the ModelDownloader.
#'
#' @param sc A spark context for interfacing between python and java
#' @param localPath The folder to save models to
#' @param serverURL The location of the model Server, beware this default can change!
#' @param ... Optional arguments; currently unused.
#'
#' @family Model downloader
#'
#' @export
smd_model_downloader <- function(sc, localPath, serverURL=DEFAULT_URL, ...) {
    session <- spark_session(sc)
    env <- new.env(parent = emptyenv())
    env$model <- "com.microsoft.azure.synapse.ml.downloader.ModelDownloader"
    downloader <- invoke_new(sc, env$model, session, localPath, serverURL)
}

#' Downloads the model by given name
#'
#' @param smd_model_downloader The model downloader
#' @param name The name of the model
#' @param ... Optional arguments; currently unused.
#'
#' @family Model downloader
#'
#' @export
smd_download_by_name <- function(model_downloader, name, ...) {
    model <- invoke(model_downloader, "downloadByName", name)
}

#' Downloads models stored locally on the filesystem
#'
#' @param smd_model_downloader The model downloader
#' @param ... Optional arguments; currently unused.
#'
#' @family Model downloader
#'
#' @export
smd_local_models <- function(model_downloader, ...) {
    model <- invoke(model_downloader, "localModels")
}

#' Downloads models stored remotely.
#'
#' @param smd_model_downloader The model downloader
#' @param ... Optional arguments; currently unused.
#'
#' @family Model downloader
#'
#' @export
smd_remote_models <- function(model_downloader, ...) {
    model <- invoke(model_downloader, "remoteModels")
}

#' Gets the name of the downloaded model
#'
#' @param model The downloaded model
#' @param ... Optional arguments; currently unused.
#'
#' @family Model downloader
#'
#' @export
smd_get_model_name <- function(model, ...) {
    name <- invoke(model, "name")
}

#' Gets the location of the model's bytes
#'
#' @param model The downloaded model
#' @param ... Optional arguments; currently unused.
#'
#' @family Model downloader
#'
#' @export
smd_get_model_uri <- function(model, ...) {
    uri <- invoke(invoke(model, "uri"), "toString")
}

#' Gets the domain that the model operates on
#'
#' @param model The downloaded model
#' @param ... Optional arguments; currently unused.
#'
#' @family Model downloader
#'
#' @export
smd_get_model_type <- function(model, ...) {
    name <- invoke(model, "modelType")
}

#' Gets the sha256 hash of the models bytes
#'
#' @param model The downloaded model
#' @param ... Optional arguments; currently unused.
#'
#' @family Model downloader
#'
#' @export
smd_get_model_hash <- function(model, ...) {
    name <- invoke(model, "hash")
}

#' Gets the size of the model in bytes
#'
#' @param model The downloaded model
#' @param ... Optional arguments; currently unused.
#'
#' @family Model downloader
#'
#' @export
smd_get_model_size <- function(model, ...) {
    name <- invoke(model, "size")
}

#' Gets the node which represents the input
#'
#' @param model The downloaded model
#' @param ... Optional arguments; currently unused.
#'
#' @family Model downloader
#'
#' @export
smd_get_model_input_node <- function(model, ...) {
    name <- invoke(model, "inputNode")
}

#' Gets the number of layers of the model
#'
#' @param model The downloaded model
#' @param ... Optional arguments; currently unused.
#'
#' @family Model downloader
#'
#' @export
smd_get_model_num_layers <- function(model, ...) {
    name <- invoke(model, "numLayers")
}

#' Gets the names of nodes that represent layers in the network
#'
#' @param model The downloaded model
#' @param ... Optional arguments; currently unused.
#'
#' @family Model downloader
#'
#' @export
smd_get_model_layer_names <- function(model, ...) {
    name <- invoke(model, "layerNames")
}
