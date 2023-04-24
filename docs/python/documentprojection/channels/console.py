from framework import *
from utils.logging import get_log
from framework.markdown import MarkdownFormatter

log = get_log(__name__)

# A sample Console (no-operation) channel that 'publishes' to the console. Useful for testing.
class ConsoleDoc(Document):
    def __init__(self, content, metadata):
        self.content = content
        self.metadata = metadata


class ConsoleFormatter(MarkdownFormatter):
    def clean_markdown(self, markdown: str) -> str:
        return markdown

    def get_header(self, notebook: Notebook) -> str:
        return "This is a test header injected by the 'console' formatter."

    def get_metadata(self, notebook: Notebook) -> dict:
        return {"source_path": notebook.path, "target_path": "stdout"}


class ConsolePublisher(Publisher):
    def publish(self, document: Document) -> bool:
        print(document.content)
        return True


class ConsoleChannel(Channel):
    def __init__(self):
        self.formatter = ConsoleFormatter()
        self.publisher = ConsolePublisher()
