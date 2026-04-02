# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

"""
Compatibility helpers for third-party dependencies that removed legacy
surfaces needed by mlflow when running SynapseML's Python tests.
"""

from __future__ import annotations

import sys
import types
from typing import Any


def _register_module(module: types.ModuleType) -> None:
    """Register a synthetic module under google.protobuf.* if needed."""
    sys.modules[module.__name__] = module
    parent_name, _, child_name = module.__name__.rpartition(".")
    if not parent_name:
        return
    parent = sys.modules.get(parent_name)
    if parent is None:
        parent = types.ModuleType(parent_name)
        sys.modules[parent_name] = parent
    setattr(parent, child_name, module)


def ensure_protobuf_service_module() -> None:
    """
    Re-introduce the deprecated ``google.protobuf.service`` module when running
    with protobuf >= 6.0, which deleted the file entirely. mlflow still imports
    it via ``from google.protobuf import service`` when deserialising its proto
    descriptors, so we provide a tiny shim that mirrors the historical API.
    """

    try:
        import google.protobuf.service  # type: ignore  # noqa: F401

        return
    except ImportError:
        pass

    try:
        import google.protobuf  # noqa: F401
    except ImportError:
        # Nothing to do if protobuf itself is unavailable.
        return

    module = types.ModuleType("google.protobuf.service")
    module.__doc__ = (
        "Compatibility shim for the deprecated google.protobuf.service module. "
        "This minimal implementation provides the RpcException, Service, "
        "RpcController, and RpcChannel classes expected by consumers such as "
        "mlflow's generated service_pb2 module."
    )

    class RpcException(Exception):
        """Exception raised on failed blocking RPC method call."""

    class Service:
        """Abstract base interface for protocol-buffer-based RPC services."""

        def GetDescriptor(self) -> Any:
            raise NotImplementedError

        def CallMethod(
            self, method_descriptor: Any, rpc_controller: Any, request: Any, done: Any
        ) -> Any:
            raise NotImplementedError

        def GetRequestClass(self, method_descriptor: Any) -> Any:
            raise NotImplementedError

        def GetResponseClass(self, method_descriptor: Any) -> Any:
            raise NotImplementedError

    class RpcController:
        """An RpcController mediates a single method call."""

        # Client-side helpers
        def Reset(self) -> None:
            raise NotImplementedError

        def Failed(self) -> bool:
            raise NotImplementedError

        def ErrorText(self) -> str:
            raise NotImplementedError

        def StartCancel(self) -> None:
            raise NotImplementedError

        # Server-side helpers
        def SetFailed(self, reason: str) -> None:
            raise NotImplementedError

        def IsCanceled(self) -> bool:
            raise NotImplementedError

        def NotifyOnCancel(self, callback: Any) -> None:
            raise NotImplementedError

    class RpcChannel:
        """Abstract interface for an RPC channel."""

        def CallMethod(
            self,
            method_descriptor: Any,
            rpc_controller: Any,
            request: Any,
            response_class: Any,
            done: Any,
        ) -> Any:
            raise NotImplementedError

    module.RpcException = RpcException
    module.Service = Service
    module.RpcController = RpcController
    module.RpcChannel = RpcChannel

    _register_module(module)
