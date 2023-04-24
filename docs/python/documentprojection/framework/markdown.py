from abc import ABC, abstractmethod
from nbconvert import MarkdownExporter

from ..framework.objects import *

# Sample base formatter for documents that are projected to markdown, include some cleaning, and inject a header
class MarkdownFormatter(ABC):
    def _add_header(markdown: str, header: str):
        content = f"{header}\n{markdown}"
        return content

    def _to_markdown(notebook: Notebook) -> str:
        exporter = MarkdownExporter()
        markdown, _ = exporter.from_notebook_node(notebook.data)
        return markdown

    @abstractmethod
    def clean_markdown(self, markdown: str) -> str:
        pass

    @abstractmethod
    def get_header(self, notebook: Notebook) -> str:
        pass

    def format(self, notebook: Notebook) -> Document:
        markdown = MarkdownFormatter._to_markdown(notebook)
        markdown = self.clean_markdown(markdown)
        content = MarkdownFormatter._add_header(markdown, self.get_header(notebook))
        return Document(content, self.get_metadata(notebook))
