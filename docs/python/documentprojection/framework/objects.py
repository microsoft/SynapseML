from abc import ABC, abstractmethod
from ..utils.notebook import *
from ..utils.logging import get_log


def _defaultrepr(cls):
    def __repr__(self):
        return type(self).__name__

    cls.__repr__ = __repr__
    return cls


class Notebook:
    def __init__(self, path):
        self.path: str = path
        self.data: NotebookNode = Notebook._parse(self.path)
        self._repr = "Notebook(...{})".format("\\".join(self.path.split("\\")[-3:]))

    def __repr__(self):
        return self._repr

    def _parse(path: str) -> NotebookNode:
        return read(path, as_version=4)


class DocumentMetadata:
    def __init__(self, source_path: str, target_path: str, dimensions: dict = {}):
        self.source_path = source_path
        self.target_path = target_path
        self.dimensions = dimensions
        self.dimensions["target_path"] = target_path

    def __repr__(self):
        return f"{repr(self.source_path)}:{repr(self.dimensions)}"


class Document:
    def __init__(self, content, metadata: DocumentMetadata):
        self.content = content
        self.metadata = metadata

    def __repr__(self):
        return f"{repr(self.metadata)}"


@_defaultrepr
class Formatter(ABC):
    @abstractmethod
    def format(self, notebook: Notebook) -> Document:
        pass

    @abstractmethod
    def get_metadata(self, notebook: Notebook) -> DocumentMetadata:
        pass


@_defaultrepr
class Publisher(ABC):
    @abstractmethod
    def publish(self, document: Document) -> bool:
        pass


class ChannelConfig:
    def __init__(self, dict: dict):
        self.__dict__.update(dict)

    def __repr__(self):
        return repr(self.__dict__)

    project_root = None


@_defaultrepr
class Channel(ABC):
    def __init__(
        self, formatter: Formatter, publisher: Publisher, config: ChannelConfig
    ):
        self.formatter: Formatter = formatter
        self.publisher: Publisher = publisher
        self.config: ChannelConfig = config

    def format(self, notebook: Notebook) -> Document:
        instance_log = get_log(self.__class__.__name__)
        instance_log.debug(f"Formatting {notebook}")
        content = self.formatter.format(notebook)
        instance_log.debug(f"Done formatting {notebook}.")
        return content

    def publish(self, document: Document) -> bool:
        instance_log = get_log(self.__class__.__name__)
        instance_log.debug(f"Publishing {document}")
        succeeded = self.publisher.publish(document)
        instance_log.debug(
            f"Publishing {document} {'SUCCEEDED' if succeeded else 'FAILED'}"
        )
