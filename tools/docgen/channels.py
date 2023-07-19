from core import Channel, ParallelChannel
import pathlib
import shutil
import os
from nbformat import NotebookNode, read
from nbconvert import MarkdownExporter
import re
from typing import List
from os.path import join, dirname, isdir, basename


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
