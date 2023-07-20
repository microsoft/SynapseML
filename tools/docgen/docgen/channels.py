from docgen.core import ParallelChannel, Channel
import pathlib
import shutil
import os
from nbformat import NotebookNode, read
from nbconvert import MarkdownExporter
import re
from typing import List
from os.path import join, dirname, isdir, basename
from traitlets.config import Config
from bs4 import BeautifulSoup
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import markdown
from markdownify import MarkdownConverter
import warnings
from datetime import datetime


class WebsiteChannel(ParallelChannel):
    def __init__(self, input_dir: str, output_dir: str):
        self.input_dir = input_dir
        self.output_dir = output_dir

    def list_input_files(self) -> List[str]:
        return list(pathlib.Path(self.input_dir).rglob("*"))

    def process(self, input_file: str, index: int) -> ():
        print(f"Processing {input_file} for website")
        output_file = os.path.join(
            self.output_dir, os.path.relpath(input_file, self.input_dir)
        )
        if str(input_file).endswith(".ipynb"):
            output_file = str(output_file).replace(".ipynb", ".md")
            parsed = read(input_file, as_version=4)
            markdown, _ = MarkdownExporter().from_notebook_node(parsed)

            markdown = re.sub(r"style=\"[\S ]*?\"", "", markdown)
            markdown = re.sub(r"<style[\S \n.]*?</style>", "", markdown)
            title = basename(input_file).replace(".ipynb", "")
            markdown = f"---\ntitle: {title}\nhide_title: true\nstatus: stable\n---\n{markdown}"

            os.makedirs(dirname(output_file), exist_ok=True)
            with open(output_file, "w+", encoding="utf-8") as f:
                f.write(markdown)
        else:
            if os.path.isdir(input_file):
                os.makedirs(output_file, exist_ok=True)
            else:
                os.makedirs(dirname(output_file), exist_ok=True)
                shutil.copy(input_file, output_file)


class FabricChannel(Channel):
    def __init__(self, input_dir: str, output_dir: str, notebooks: List[dict]):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.notebooks = notebooks
        self.hide_tag = "hide-synapse-internal"
        self.media_dir = os.path.join(self.output_dir, "media")

    def list_input_files(self) -> List[str]:
        return [n["path"] for n in self.notebooks]

    def _sentence_to_snake(self, path: str):
        return path.lower() \
            .replace(" - ", "-") \
            .replace(" ", "-") \
            .replace(",", "") \
            .replace(".ipynb", "")

    def _download_and_replace_images(self, html_soup, output_folder, relative_to):
        output_folder = output_folder.replace("/", os.sep)
        os.makedirs(output_folder, exist_ok=True)
        img_tags = html_soup.find_all('img')
        for img_tag in img_tags:
            img_url = img_tag['src']
            response = requests.get(img_url)
            if response.status_code == 200:
                img_filename = self._sentence_to_snake(img_url.split("/")[-1])
                img_path = os.path.join(output_folder, img_filename)
                with open(img_path, 'wb') as img_file:
                    img_file.write(response.content)

                img_path_rel = os.path.relpath(img_path, relative_to).replace(os.sep, "/")
                img_tag['src'] = img_path_rel  # TODO figure this out properly
                # img_tag.replace_with(
                #     f':::image type="content" source="{img_path_rel}" '
                #     f'alt-text="{img_tag.get("alt", "placeholder alt text")}":::')
            else:
                raise ValueError(f"Could not download image from {img_url}")
        return html_soup

    def _validate_metadata(self, metadata):
        required_metadata = [
            "author",
            "description",
            "ms.author",
            "ms.topic",
            "title",
        ]
        for req in required_metadata:
            assert req in metadata.keys(), f"{req} is required metadata, please add it to menifest file"

    def _generate_metadata_header(self, metadata):
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
                "ms.date is set in manifest file, the date won't be automatically updated. "
                "to update date automatically, remove ms.date from manifest file"
            )

        generated_metadata += "---\n"
        return generated_metadata

    def process(self, input_file: str, index: int) -> ():
        print(f"Processing {input_file} for fabric")
        output_file = os.path.join(self.output_dir, input_file)

        if str(input_file).endswith(".ipynb"):
            output_file = self._sentence_to_snake(str(output_file).replace(".ipynb", ".md"))
            full_input_file = os.path.join(self.input_dir, input_file)
            parsed = read(full_input_file, as_version=4)

            c = Config()
            c.TagRemovePreprocessor.remove_cell_tags = (self.hide_tag,)
            c.TagRemovePreprocessor.enabled = True
            c.MarkdownExporter.preprocessors = ["nbconvert.preprocessors.TagRemovePreprocessor"]
            md, resources = MarkdownExporter(config=c).from_notebook_node(parsed)
            html = markdown.markdown(md, extensions=["markdown.extensions.tables", "markdown.extensions.fenced_code"])

            parsed_html = BeautifulSoup(html)

            # Download images and place them in media directory while updating their links
            parsed_html = self._download_and_replace_images(
                parsed_html,
                self.media_dir + "/" + self._sentence_to_snake(input_file),
                os.path.dirname(output_file)
            )

            # Remove extra CSS styling info
            for style_tag in parsed_html.find_all('style'):
                style_tag.extract()

            def callback(el):
                return el.contents[0]["class"][0].split("-")[-1] if len(el.contents) >= 1 else None

            # Convert from HTML to MD
            new_md = MarkdownConverter(code_language_callback=callback).convert_soup(parsed_html)

            # Add a header to MD
            metadata = self.notebooks[index]["metadata"]
            self._validate_metadata(metadata)
            new_md = f"{self._generate_metadata_header(metadata)}\n{new_md}"

            os.makedirs(dirname(output_file), exist_ok=True)
            with open(output_file, "w+", encoding="utf-8") as f:
                f.write(new_md)
