import logging
from typing import Dict, Optional
import functools
import time
import uuid
from synapse.ml.core.platform.Platform import (
    running_on_synapse_internal,
    running_on_synapse,
)
from pyspark.sql.dataframe import DataFrame
from pyspark import SparkContext
from pyspark.sql import SparkSession
import json

PROTOCOL_VERSION = "0.0.1"


class CustomFormatter(logging.Formatter):
    def format(self, record):
        record.message = record.getMessage()
        if self.usesTime():
            record.asctime = self.formatTime(record, self.datefmt)
        s = self.formatMessage(record)
        return s


class SynapseMLLogger:
    def __init__(
        self,
        library_name: str = None,  # e.g SynapseML
        library_version: str = None,
        uid: str = None,
        log_level: int = logging.INFO,
    ):
        self.logger: logging.Logger = SynapseMLLogger._get_environment_logger(
            log_level=log_level
        )
        self.library_name = library_name if library_name else "SynapseML"
        self.library_version = (
            library_version if library_version else self._get_synapseml_version()
        )
        self.uid = uid if uid else f"{self.__class__.__name__}_{uuid.uuid4()}"
        self.is_executor = False if SynapseMLLogger.safe_get_spark_context() else True

    @classmethod
    def safe_get_spark_context(cls) -> SparkContext:
        try:
            spark = SparkSession.getActiveSession()
            sc = spark.sparkContext
            return sc
        except Exception:
            return None

    @classmethod
    def _round_significant(cls, num, digits):
        from math import log10, floor

        return round(num, digits - int(floor(log10(abs(num)))) - 1)

    @classmethod
    def _get_synapseml_version(cls) -> Optional[str]:
        try:
            from synapse.ml.core import __spark_package_version__

            return __spark_package_version__
        except Exception:
            return None

    def get_required_log_fields(
        self,
        uid: str,
        class_name: str,
        method: str,
    ):
        return {
            "modelUid": uid,
            "className": class_name,
            "method": method,
            "libraryVersion": self.library_version,
            "libraryName": self.library_name,
        }

    @classmethod
    def _get_environment_logger(cls, log_level: int) -> logging.Logger:
        if running_on_synapse_internal():
            from synapse.ml.pymds.synapse_logger import get_mds_logger

            return get_mds_logger(
                name=__name__,
                log_level=log_level,
                formatter=CustomFormatter(fmt="%(message)s"),
            )
        elif running_on_synapse():
            logger = logging.getLogger(__name__)
            logger.setLevel(log_level)
            return logger
        else:
            logger = logging.getLogger(__name__)
            logger.setLevel(log_level)
            return logger

    @classmethod
    def get_hadoop_conf_entries(cls):
        if running_on_synapse_internal():
            from synapse.ml.internal_utils.session_utils import get_fabric_context

            return {
                "artifactId": get_fabric_context().get("trident.artifact.id"),
                "workspaceId": get_fabric_context().get("trident.workspace.id"),
                "capacityId": get_fabric_context().get("trident.capacity.id"),
                "artifactWorkspaceId": get_fabric_context().get(
                    "trident.artifact.workspace.id"
                ),
                "livyId": get_fabric_context().get("trident.activity.id"),
                "artifactType": get_fabric_context().get("trident.artifact.type"),
                "tenantId": get_fabric_context().get("trident.tenant.id"),
                "lakehouseId": get_fabric_context().get("trident.lakehouse.id"),
            }
        else:
            return {}

    def _log_base(
        self,
        class_name: str,
        method_name: Optional[str],
        num_cols: int,
        execution_sec: float,
        feature_name: Optional[str] = None,
    ):
        self._log_base_dict(
            self._get_payload(class_name, method_name, num_cols, execution_sec, None),
            feature_name=feature_name,
        )

    def _log_base_dict(self, info: Dict[str, str], feature_name: Optional[str] = None):
        if feature_name is not None and running_on_synapse_internal():
            from synapse.ml.fabric.telemetry_utils import report_usage_telemetry

            keys_to_remove = ["libraryName", "method"]
            attributes = {
                key: value for key, value in info.items() if key not in keys_to_remove
            }
            report_usage_telemetry(
                feature_name=self.library_name,
                activity_name=feature_name,
                attributes=attributes,
            )
        self.logger.info(json.dumps(info))

    def log_message(self, message: str):
        self.logger.info(message)

    @classmethod
    def get_error_fields(cls, e: Exception) -> Dict[str, str]:
        return {"errorType": str(type(e)), "errorMessage": f"{e}"}

    def _get_payload(
        self,
        class_name: str,
        method_name: Optional[str],
        num_cols: Optional[int],
        execution_sec: Optional[float],
        exception: Optional[Exception],
    ):
        info = self.get_required_log_fields(self.uid, class_name, method_name)
        env_conf = self.get_hadoop_conf_entries()
        for k in env_conf.keys():
            info[k] = env_conf[k]
        if num_cols is not None:
            info["dfInfo"] = {"input": {"numCols": str(num_cols)}}
        if execution_sec is not None:
            info["executionSeconds"] = str(execution_sec)
        if exception:
            exception_info = self.get_error_fields(exception)
            for k in exception_info.keys():
                info[k] = exception_info[k]
        info["protocolVersion"] = PROTOCOL_VERSION
        info["isExecutor"] = self.is_executor
        return info

    def _log_error_base(self, class_name: str, method_name: str, e: Exception):
        self.logger.exception(
            json.dumps(self._get_payload(class_name, method_name, None, None, e))
        )

    def log_verb(method_name: Optional[str] = None):
        def get_wrapper(func):
            @functools.wraps(func)
            def log_decorator_wrapper(self, *args, **kwargs):
                start_time = time.perf_counter()
                try:
                    result = func(self, *args, **kwargs)
                    execution_time = SynapseMLLogger._round_significant(
                        time.perf_counter() - start_time, 3
                    )
                    self._log_base(
                        func.__module__,
                        method_name if method_name else func.__name__,
                        SynapseMLLogger.get_column_number(args, kwargs),
                        execution_time,
                        None,
                    )
                    return result
                except Exception as e:
                    self._log_error_base(
                        func.__module__,
                        method_name if method_name else func.__name__,
                        e,
                    )
                    raise

            return log_decorator_wrapper

        return get_wrapper

    def log_class(self, feature_name: str):
        return self._log_base("constructor", None, None, None, feature_name)

    def log_transform():
        return SynapseMLLogger.log_verb("transform")

    def log_fit():
        return SynapseMLLogger.log_verb("fit")

    @classmethod
    def get_column_number(cls, args, kwargs):
        if kwargs and kwargs["df"] and isinstance(kwargs["df"], DataFrame):
            return len(kwargs["df"].columns)
        elif args and len(args) > 0 and isinstance(args[0], DataFrame):
            return len(args[0].columns)
        return None
