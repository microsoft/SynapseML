# OnnxMl.java

This file was compiled from the onnx-ml.proto file in ONNX v1.9.0 using protoc 3.17.2.

The onnx-ml.proto file is part of the ONNX project and licensed under Apache 2.0.
https://github.com/onnx/onnx/blob/main/onnx/onnx-ml.proto

We are including this file in order to enable manual manipulation of ONNX protobufs.
The ONNX Runtime does not allow slicing models at intermediate nodes, which is an
important scenario for image featurization. This code allows us to manipulate the model at
a more detailed level.