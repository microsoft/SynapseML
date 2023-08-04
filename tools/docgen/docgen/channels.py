import os
import pathlib
import re
import shutil
import warnings
from datetime import datetime
from os.path import basename, dirname
from typing import List
from urllib.parse import urlparse

import markdown
import pypandoc
import requests
from bs4 import BeautifulSoup
from docgen.core import Channel, ParallelChannel
from markdownify import ATX, MarkdownConverter
from nbconvert import MarkdownExporter
from nbformat import read
from traitlets.config import Config


class WebsiteChannel(ParallelChannel):
    def __init__(self, input_dir: str, output_dir: str):
        self.input_dir = input_dir
        self.output_dir = output_dir

    def list_input_files(self) -> List[str]:
        return list(pathlib.Path(self.input_dir).rglob("*"))

    def process(self, input_file: str) -> ():
        print(f"Processing {input_file} for website")
        output_file = os.path.join(
            self.output_dir, os.path.relpath(input_file, self.input_dir)
        )
        if str(input_file).endswith(".ipynb"):
            output_file = str(output_file).replace(".ipynb", ".md")
            parsed = read(input_file, as_version=4)
            markdown, resources = MarkdownExporter().from_notebook_node(parsed)

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
        return (
            path.lower()
            .replace(" - ", "-")
            .replace(" ", "-")
            .replace(",", "")
            .replace(".ipynb", "")
            .replace(".rst", "")
        )

    def _is_valid_url(self, url):
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False

    def _replace_img_tag(self, img_tag, img_path_rel):
        img_tag.replace_with(
            f':::image type="content" source="{img_path_rel}" '
            f'alt-text="{img_tag.get("alt", "placeholder alt text")}":::'
        )

    def _download_and_replace_images(
        self,
        html_soup,
        resources,
        output_folder,
        relative_to,
        notebook_path,
        get_image_from_local=False,
    ):
        output_folder = output_folder.replace("/", os.sep)
        os.makedirs(output_folder, exist_ok=True)

        if resources:
            # resources converted from notebook
            resources_img, i = [], 0
            for img_filename, content in resources.get("outputs", {}).items():
                img_path = os.path.join(output_folder, img_filename.replace("_", "-"))
                with open(img_path, "wb") as img_file:
                    img_file.write(content)
                img_path_rel = os.path.relpath(img_path, relative_to).replace(
                    os.sep, "/"
                )
                resources_img.append(img_path_rel)

        img_tags = html_soup.find_all("img")
        for img_tag in img_tags:
            img_loc = img_tag["src"]

            if self._is_valid_url(img_loc):
                # downloaded image
                response = requests.get(img_loc)
                if response.status_code == 200:
                    img_filename = self._sentence_to_snake(img_loc.split("/")[-1])
                    img_path = os.path.join(output_folder, img_filename)
                    with open(img_path, "wb") as img_file:
                        img_file.write(response.content)
                    img_path_rel = os.path.relpath(img_path, relative_to).replace(
                        os.sep, "/"
                    )
                    img_tag["src"] = img_path_rel
                else:
                    raise ValueError(f"Could not download image from {img_loc}")

            elif get_image_from_local:
                # process local images
                img_filename = self._sentence_to_snake(img_loc.split("/")[-1]).replace(
                    "_", "-"
                )
                file_folder = "/".join(
                    notebook_path.split("/")[:-1]
                )  # path read from manifest file
                img_input_path = os.path.join(
                    self.input_dir, file_folder, img_loc
                ).replace("/", os.sep)
                if not os.path.exists(img_input_path):
                    raise ValueError(f"Could not get image from {img_loc}")
                img_path = os.path.join(output_folder, img_filename)
                img_path_rel = os.path.relpath(img_path, relative_to).replace(
                    os.sep, "/"
                )
                shutil.copy(img_input_path, img_path)

            else:
                # process image got from notebook resources
                img_path_rel = resources_img[i]
                img_tag["src"] = img_path_rel
                i += 1

            self._replace_img_tag(img_tag, img_path_rel)

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
            assert (
                req in metadata.keys()
            ), f"{req} is required metadata, please add it to manifest file"

    def _generate_metadata_header(self, metadata):
        """
        take a file and the authors name, generate metadata
        metadata requirements: https://learn.microsoft.com/contribute/metadata
        Azure Doc require MS authors and contributors need to make content contributions through the private repository
        so the content can be staged and validated by the current validation rules. (Jan 4th, 2023)
        """
        if "ms.date" not in metadata:
            update_date = datetime.today().strftime("%m/%d/%Y")
            metadata["ms.date"] = update_date
        else:
            warnings.warn(
                "ms.date is set in manifest file, the date won't be automatically updated. "
                "to update date automatically, remove ms.date from manifest file"
            )
        formatted_list = (
            ["---"]
            + ["{k}: {v}".format(k=k, v=v) for k, v in metadata.items()]
            + ["---\n"]
        )
        return "\n".join(formatted_list)

    def _remove_content(self, text):
        patterns_to_remove = [
            "https://docs.microsoft.com",
            "https://learn.microsoft.com",
        ]
        for pattern in patterns_to_remove:
            text = re.sub(pattern, "", text)
        return text

    def _read_rst(self, rst_file_path):
        try:
            extra_args = ["--wrap=none"]
            html_string = pypandoc.convert_file(
                rst_file_path, "html", format="rst", extra_args=extra_args
            )
            return html_string
        except Exception as e:
            print("Error converting the RST file to Markdown:", e)
            return None

    def _convert_to_markdown_links(self, parsed_html):
        for link in parsed_html.find_all("a", href=True):
            href = link["href"]
            if not self._is_valid_url(href) and ".md" not in href:
                split_href = href.split("#")
                split_href[0] += ".md"
                new_href = "#".join(split_href)
                link["href"] = new_href
        return parsed_html

    def process(self, input_file: str, index: int) -> ():
        print(f"Processing {input_file} for fabric")
        output_file = os.path.join(self.output_dir, input_file)
        output_img_dir = self.media_dir + "/" + self._sentence_to_snake(input_file)
        full_input_file = os.path.join(self.input_dir, input_file)
        notebook_path = self.notebooks[index]["path"]
        metadata = self.notebooks[index]["metadata"]
        self._validate_metadata(metadata)

        def callback(el):
            if el.contents[0].has_attr("class"):
                return (
                    el.contents[0]["class"][0].split("-")[-1]
                    if len(el.contents) >= 1
                    else None
                )
            else:
                return el["class"][0] if el.has_attr("class") else None

        def convert_soup_to_md(soup, **options):
            return MarkdownConverter(**options).convert_soup(soup)

        if str(input_file).endswith(".rst"):
            output_file = self._sentence_to_snake(
                str(output_file).replace(".rst", ".md")
            )
            html = self._read_rst(full_input_file)
            parsed_html = markdown.markdown(
                html,
                extensions=[
                    "markdown.extensions.tables",
                    "markdown.extensions.fenced_code",
                ],
            )
            parsed_html = BeautifulSoup(parsed_html)
            parsed_html = self._download_and_replace_images(
                parsed_html,
                None,
                output_img_dir,
                os.path.dirname(output_file),
                notebook_path,
                True,
            )
            parsed_html = self._convert_to_markdown_links(parsed_html)

        elif str(input_file).endswith(".ipynb"):
            output_file = self._sentence_to_snake(
                str(output_file).replace(".ipynb", ".md")
            )
            parsed = read(full_input_file, as_version=4)

            c = Config()
            c.TagRemovePreprocessor.remove_cell_tags = (self.hide_tag,)
            c.TagRemovePreprocessor.enabled = True
            c.MarkdownExporter.preprocessors = [
                "nbconvert.preprocessors.TagRemovePreprocessor"
            ]
            md, resources = MarkdownExporter(config=c).from_notebook_node(parsed)

            html = markdown.markdown(
                md,
                extensions=[
                    "markdown.extensions.tables",
                    "markdown.extensions.fenced_code",
                ],
            )
            parsed_html = BeautifulSoup(html)
            # Download images and place them in media directory while updating their links
            parsed_html = self._download_and_replace_images(
                parsed_html,
                resources,
                output_img_dir,
                os.path.dirname(output_file),
                None,
                False,
            )
            # Remove StatementMeta
            for element in parsed_html.find_all(
                text=re.compile("StatementMeta\(.*?Available\)")
            ):
                element.extract()

            # Remove extra CSS styling info
            for style_tag in parsed_html.find_all("style"):
                style_tag.extract()

        # Convert from HTML to MD
        new_md = convert_soup_to_md(
            parsed_html,
            code_language_callback=callback,
            heading_style=ATX,
            escape_underscores=False,
        )
        # Post processing
        new_md = f"{self._generate_metadata_header(metadata)}\n{new_md}"
        output_md = self._remove_content(new_md)

        os.makedirs(dirname(output_file), exist_ok=True)
        with open(output_file, "w+", encoding="utf-8") as f:
            f.write(output_md)
