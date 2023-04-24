import os
import re

from framework import *
from utils.logging import get_log
from framework.markdown import MarkdownFormatter

log = get_log(__name__)


def get_project_root() -> str:
    """Returns project root folder."""
    # root of parent module
    filepath = Path(os.path.abspath(os.path.join(os.getcwd(), __file__)))
    return os.path.abspath(filepath.parent.parent.parent.parent.parent)


class WebsiteDoc(Document):
    def __init__(self, content, metadata):
        self.content = content
        self.metadata = metadata


class WebsiteFormatter(MarkdownFormatter):
    def clean_markdown(self, markdown: str) -> str:
        markdown = re.sub(r"style=\"[\S ]*?\"", "", markdown)
        markdown = re.sub(r"<style[\S \n.]*?</style>", "", markdown)
        return markdown

    def get_header(self, notebook: Notebook) -> str:
        filename = os.path.basename(notebook.path).replace(".ipynb", "")
        return f"---\ntitle: {filename}\nhide_title: true\nstatus: stable\n---"

    def get_metadata(self, notebook: Notebook) -> DocumentMetadata:
        feature_dir = os.path.basename(os.path.dirname(notebook.path))
        file_name = os.path.basename(notebook.path).replace("ipynb", "md")
        website_path = os.path.join(
            get_project_root(), "website", "docs", "features", feature_dir, file_name
        )
        return DocumentMetadata(notebook.path, website_path)


class WebsitePublisher(Publisher):
    def publish(self, document: Document) -> bool:
        with open(document.metadata.target_path, "w", encoding="utf-8") as f:
            f.write(document.content)

    # TODO: run orchestrate yarn


class WebsiteChannel(Channel):
    def __init__(self):
        self.formatter = WebsiteFormatter()
        self.publisher = WebsitePublisher()
