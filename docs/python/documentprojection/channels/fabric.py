import os
import re
import warnings
from datetime import datetime

import requests
from nbconvert import MarkdownExporter
from traitlets.config import Config

from ..framework import *
from ..framework.markdown import MarkdownFormatter
from ..utils.logging import get_log

log = get_log(__name__)

class FabricDoc(Document):
    def __init__(self, content, metadata):
        self.content = content
        self.metadata = metadata

class FabricPublisher(Publisher):
    def __init__(self, config: ChannelMetadata):
        self.config = config
    
    def get_config(self):
        return self.config
    
    def publish(self, document: Document) -> bool:
        if not document.metadata['is_active']:
            return False
        output_dir = self.get_config()['output_dir']
        make_dir(output_dir)
        filename = document.metadata['filename']+'.md'
        print(filename)
        with open(os.path.join(output_dir, filename), "w", encoding="utf-8") as f:
            f.write(document.content)
        return True

class FabricChannel(Channel):
    def __init__(self, config: ChannelMetadata):
        self.formatter = FabricFormatter(config)
        self.publisher = FabricPublisher(config)

class FabricFormatter(MarkdownFormatter):
    def __init__(self, config: ChannelMetadata):
        self.config = config

    def _to_markdown(notebook: Notebook, hide_tag="hide-synapse-internal") -> str:
        c = Config()
        c.TagRemovePreprocessor.remove_cell_tags = (hide_tag,)
        c.TagRemovePreprocessor.enabled = True
        c.MarkdownExporter.preprocessors = ["nbconvert.preprocessors.TagRemovePreprocessor"]
        exporter = MarkdownExporter(config=c)
        markdown, _ = exporter.from_notebook_node(notebook.data)
        return markdown
    
    def get_config(self):
        return self.config
    
    def get_header(self, notebook: Notebook) -> str:
        fabric_metadata = self.get_metadata(notebook)['metadata']
        self.check_required_metadata(fabric_metadata)
        generated_metadata = self.generate_metadata(fabric_metadata)
        return generated_metadata

    def get_metadata(self, notebook: Notebook) -> dict:
        notebook.metadata.update(
            {"source_path": notebook.path, "target_path": "stdout"}
        )
        return notebook.metadata
    
    def clean_markdown(self, markdown: str, metadata) -> str:
        filename = metadata['filename']
        try:
            alt_texts = metadata["alt_texts"]
        except KeyError:
            alt_texts = []
        try:
            manifest_mapping = metadata["replace_mapping"]
        except KeyError:
            manifest_mapping = {}
        try:
            footer = metadata["footer"]
        except KeyError:
            footer = ""
        media_dir = self.get_config()['media_dir']
        output_dir = self.get_config()['output_dir']
        markdown = process_img(markdown, filename, output_dir, media_dir, alt_texts)
        markdown = self.combine_documentation(markdown, footer, manifest_mapping)
        return markdown
    
    def format(self, notebook: Notebook) -> Document:
        markdown = FabricFormatter._to_markdown(notebook)
        metadata = self.get_metadata(notebook)
        markdown = self.clean_markdown(markdown, metadata)
        markdown = FabricFormatter._add_header(markdown, self.get_header(notebook))
        return Document(markdown, self.get_metadata(notebook))
    

    def check_required_metadata(self, metadata):
        for required_metadata in {
            "author",
            "description",
            "ms.author",
            "ms.topic",
            "title",
        }:
            if required_metadata not in metadata:
                raise ValueError(
                    "{required_metadata} is required metadata, please add it to menifest file".format(
                        required_metadata=required_metadata
                    )
                )

    def generate_metadata(self, metadata):
        """
        take a file and the authors name, generate metadata
        metadata requirements: https://learn.microsoft.com/en-us/contribute/metadata
        Azure Doc require MS authors and contributors need to make content contributions through the private repository
        so the content can be staged and validated by the current validation rules. (Jan 4th, 2023)
        """
        generated_metadata = "---\n"
        for k, v in metadata.items():
            generated_metadata += "{k}: {v}\n".format(k=k, v=v)
        if "ms.date" not in metadata:
            update_date = datetime.today().strftime("%m/%d/%Y")
            generated_metadata += "ms.date: {update_date}\n".format(
                update_date=update_date
            )
        else:
            warnings.warn(
                "ms.date is set in manifest file, the date won't be automatically updated. to update date automatically, remove ms.date from manifest file"
            )

        generated_metadata += "---\n"
        return generated_metadata

    def combine_documentation(self, body, footer, manifest_mapping):
        if footer:
            with open(footer, "r") as f:
                end = f.read()
        else:
            end = ""
        generated_doc = "".join([body, end])
        generated_doc = remove_replace_content(generated_doc, manifest_mapping)
        return generated_doc


def download_image(image_url, image_path):
    response = requests.get(image_url)
    image_content = response.content
    with open(image_path, "wb") as f:
        f.write(image_content)

def rename(file_name):
    """
    rename filename to meet Fabric doc requirement
    """
    file_name = file_name.replace("_", "-").lower()
    return file_name.replace(" ", "-").lower()

def process_img(nb_body, folder_name, output_dir, media_dir, alt_texts):
    """
    handle Azure doc validation rule (Suggestion: external-image, Warning: alt-text-missing)
    scan text and find external image link, download the image and store in the img folder
    replace image link with img path.
    alt texts are passed in from manifest
    """
    image_tags = re.finditer(r"<img.*?>|<image.*?>", nb_body)
    process_nb_body = []
    prev = 0
    alt_text_count = 0
    for match in image_tags:
        start_index = match.start()
        end_index = match.end()
        content = nb_body[prev:start_index]
        process_nb_body.append(content)
        url = re.search(
            r"<(?:img|image).*?src=\"(.*?)\".*?>", nb_body[start_index:end_index]
        ).group(1)
        file_name = url.split("/")[-1]
        file_dir = "/".join([output_dir, media_dir, folder_name])
        make_dir(file_dir)
        img_azure_doc_path = "/".join([folder_name, rename(file_name)])
        md_img_input_path = "/".join([media_dir, img_azure_doc_path])
        file_path = "/".join([output_dir, media_dir, img_azure_doc_path])
        download_image(url, file_path)
        alt_text = alt_texts[alt_text_count]
        alt_text_count += 1
        md_img_path = ':::image source="{img_path}" alt-text="{alt_text}":::'.format(
            img_path=md_img_input_path, alt_text=alt_text
        )
        process_nb_body.append(md_img_path)
        prev = end_index
    process_nb_body.append(nb_body[prev:])
    return "".join(process_nb_body)

def make_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def remove_replace_content(text, manifest_mapping):
    replace_mapping = {"from synapse.ml.core.platform import materializing_display as display":"",
                        "https://docs.microsoft.com":"", 
                        "https://learn.microsoft.com":""}
    replace_mapping.update(manifest_mapping)
    for ori, new in replace_mapping.items():
        text = text.replace(ori, new)
    return text