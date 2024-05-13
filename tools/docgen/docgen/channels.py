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
from docgen.fabric_helpers import LearnDocPreprocessor, HTMLFormatter, sentence_to_snake
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
    def __init__(
        self,
        input_dir: str,
        output_dir: str,
        notebooks: List[dict],
        output_structure,
        auto_pre_req,
    ):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.notebooks = notebooks
        self.output_structure = output_structure
        self.auto_pre_req = auto_pre_req
        self.channel = "fabric"
        self.hide_tag = "hide-synapse-internal"
        self.media_dir = os.path.join(self.output_dir, "media")

    def list_input_files(self) -> List[str]:
        return [n["path"] for n in self.notebooks]

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

    def _generate_related_content(self, index, output_file):
        related_content_index = index + 1
        max_index = len(self.notebooks)
        related_content = []
        if max_index > 3:
            related_content = ["""## Related content\n"""]
            for i in range(3):
                if related_content_index >= max_index:
                    related_content_index = 0
                title = self.notebooks[related_content_index]["metadata"]["title"]
                if self.output_structure == "hierarchy":
                    path = self.notebooks[related_content_index]["path"]
                    filename = sentence_to_snake(
                        self.output_dir
                        + self.notebooks[related_content_index].get("filename", path)
                        + ".md"
                    )
                    rel_path = os.path.relpath(
                        filename, os.path.dirname(output_file)
                    ).replace(os.sep, "/")
                    related_content.append(f"""- [{title}]({rel_path})""")
                elif self.output_structure == "flat":
                    path = self.notebooks[related_content_index]["path"].split("/")[-1]
                    filename = sentence_to_snake(
                        self.notebooks[related_content_index].get("filename", path)
                        + ".md"
                    )
                    related_content.append(f"""- [{title}]({filename})""")
                related_content_index += 1
        return "\n".join(related_content)

    def process(self, input_file: str, index: int) -> ():
        print(f"Processing {input_file} for {self.channel}")
        full_input_file = os.path.join(self.input_dir, input_file)
        notebook_path = self.notebooks[index]["path"]
        manifest_file_name = self.notebooks[index].get("filename", "")
        metadata = self.notebooks[index]["metadata"]

        if self.output_structure == "hierarchy":
            # keep structure of input file
            output_file = os.path.join(self.output_dir, input_file)
            output_img_dir = os.path.join(self.media_dir, sentence_to_snake(input_file))
        elif self.output_structure == "flat":
            # put under one directory
            media_folder = (
                manifest_file_name
                if manifest_file_name
                else sentence_to_snake(input_file.split("/")[-1])
            )
            output_img_dir = os.path.join(self.media_dir, media_folder)
            output_file = os.path.join(self.output_dir, input_file.split("/")[-1])

        if manifest_file_name:
            output_file = output_file.replace(
                output_file.split("/")[-1].split(".")[0], manifest_file_name
            )

        auto_related_content = self._generate_related_content(index, output_file)
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
            output_file = sentence_to_snake(str(output_file).replace(".rst", ".md"))
            content = self._read_rst(full_input_file)
            html = HTMLFormatter(
                content,
                resources=resources,
                input_dir=self.input_dir,
                notebook_path=input_file,
                output_img_dir=output_img_dir,
                output_file=output_file,
            )
            parsed_html = self._convert_to_markdown_links(parsed_html)

        elif str(input_file).endswith(".ipynb"):
            output_file = sentence_to_snake(str(output_file).replace(".ipynb", ".md"))
            parsed = read(full_input_file, as_version=4)

            c = Config()
            c.MarkdownExporter.preprocessors = [
                LearnDocPreprocessor(
                    tags_to_remove=[self.hide_tag], auto_pre_req=self.auto_pre_req
                )
            ]
            content, resources = MarkdownExporter(config=c).from_notebook_node(parsed)

            html = HTMLFormatter(
                content,
                resources=resources,
                input_dir=self.input_dir,
                notebook_path=input_file,
                output_img_dir=output_img_dir,
                output_file=output_file,
            )
            html.run()
            parsed_html = html.bs_html

        # Remove StatementMeta
        for element in parsed_html.find_all(
            text=re.compile("StatementMeta\(.*?Available\)")
        ):
            element.extract()
            warnings.warn(
                f"Found StatementMeta in {input_file}, please check if you want it in the notebook.",
                UserWarning,
            )

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
        if "## Related content" not in new_md:
            new_md += auto_related_content
        output_md = re.sub(r"\n{3,}", "\n\n", self._remove_content(new_md))

        os.makedirs(dirname(output_file), exist_ok=True)
        with open(output_file, "w+", encoding="utf-8") as f:
            f.write(output_md)


class AzureChannel(FabricChannel):
    def __init__(
        self,
        input_dir: str,
        output_dir: str,
        notebooks: List[dict],
        output_structure,
        auto_pre_req,
    ):
        super().__init__(
            input_dir, output_dir, notebooks, output_structure, auto_pre_req
        )
        self.hide_tag = "hide-azure"
        self.channel = "azure"
