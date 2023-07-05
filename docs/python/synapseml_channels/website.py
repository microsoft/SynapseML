import os
import re

from documentprojection.framework import *
from documentprojection.utils.logging import get_log
from documentprojection.framework.markdown import MarkdownFormatter
from documentprojection.utils.parallelism import get_lock

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
    def __init__(self, config: ChannelMetadata):
        self.config = config

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
            self.config.project_root,
            "website",
            "docs",
            "features",
            feature_dir,
            file_name,
        )
        return DocumentMetadata(notebook.path, website_path)


class WebsitePublisher(Publisher):
    def publish(self, document: Document) -> bool:
        dir_name = os.path.dirname(document.metadata.target_path)
        with get_lock(dir_name):
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)
        with open(document.metadata.target_path, "w", encoding="utf-8") as f:
            f.write(document.content)

    # TODO: run orchestrate yarn


class WebsiteChannel(Channel):
    def __init__(self, config: ChannelMetadata):
        self.formatter = WebsiteFormatter(config)
        self.publisher = WebsitePublisher()
