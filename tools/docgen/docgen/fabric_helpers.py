import difflib
import markdown
from bs4 import BeautifulSoup
from nbconvert.preprocessors import Preprocessor
import os
from urllib.parse import urlparse
import requests
import shutil


class LearnDocPreprocessor(Preprocessor):
    def __init__(self, tags_to_remove=None, auto_pre_req=None, **kwargs):
        """
        Initializes the preprocessor with optional remove tags.
        :param remove_tags: A list of tags based on which cells will be removed.
        """
        super(LearnDocPreprocessor, self).__init__(**kwargs)
        self.tags_to_remove = tags_to_remove if tags_to_remove else []
        self.auto_pre_req = auto_pre_req

    def preprocess(self, nb, resources):
        """
        Preprocess the entire notebook, removing cells tagged with in remove tag list
        and process other cells.
        """
        if self.tags_to_remove:
            nb.cells = [
                cell
                for cell in nb.cells
                if not set(self.tags_to_remove).intersection(
                    cell.metadata.get("tags", [])
                )
            ]

        for index, cell in enumerate(nb.cells):
            nb.cells[index], resources = self.process_cell(cell, resources, index)
        return nb, resources

    def add_auto_prereqs(self):
        prerequisites = [
            "## Prerequisites\n\n[!INCLUDE [prerequisites](includes/prerequisites.md)]"
        ]
        prerequisites.append(
            "- Attach your notebook to a lakehouse. On the left side, select **Add** to add an existing lakehouse or create a lakehouse."
        )
        return "\n".join(prerequisites)

    def process_cell(self, cell, resources, index):
        """
        Adds '> ' before Markdown cells tagged with 'alert' and an alert type.
        """
        if (
            cell.cell_type == "markdown"
            and ("tags" in cell.metadata)
            and ("alert" in cell.metadata["tags"])
        ):
            for tag in cell.metadata["tags"]:
                if tag in ["note", "tip", "important", "warning", "caution"]:
                    head = f"> [!{tag.upper()}]\n"
                    cell.source = head + "\n".join(
                        "> " + line
                        for line in cell.source.splitlines()
                        if not line.startswith(f"## {tag.capitalize()}")
                    )
        if self.auto_pre_req and index == 1 and cell.cell_type == "markdown":
            cell.source = self.add_auto_prereqs() + "\n" + cell.source
        return cell, resources


class HTMLFormatter:
    def __init__(self, content, **kwargs):
        self.content = content
        self.attributes = kwargs
        self.bs_html = None
        self.resource_images_path_dict = {}
        self.resources = self.attributes.get("resources", None)
        self.input_dir = self.attributes.get("input_dir", None)
        self.notebook_path = self.attributes.get("notebook_path", None)
        self.output_img_dir = self.attributes.get("output_img_dir", None)
        self.output_file = self.attributes.get("output_file", None)

    def parse_html(self):
        extensions = ["markdown.extensions.tables", "markdown.extensions.fenced_code"]
        html_str = markdown.markdown(self.content, extensions=extensions)
        input_format = self.attributes.get("input_format", None)
        features = {"rst": "html.parser", "ipynb": None}.get(input_format, None)
        self.bs_html = BeautifulSoup(html_str, features=features)

    def manage_images(self):
        self.process_resource_images()
        for img in self.bs_html.find_all("img"):
            img_path = img.get("src")
            if img_path.startswith("http"):
                img_path_rel = self.process_external_images(
                    img_path, output_img_dir=self.output_img_dir
                )
            else:
                img_path_rel = self.process_local_images(img_path)
            img["src"] = img_path_rel
            if not img.get("alt"):
                img["alt"] = img_path_rel.split("/")[-1].split(".")[0].replace("-", " ")
            self._replace_img_tag(img, img_path_rel)

    def process_resource_images(self):
        if self.resources:
            for img_filename, content in self.resources.get("outputs", {}).items():
                img_path = os.path.join(
                    self.output_img_dir, img_filename.replace("_", "-")
                )
                with open(img_path, "wb") as img_file:
                    img_file.write(content)
                img_path_rel = os.path.relpath(
                    img_path, os.path.dirname(self.output_file)
                ).replace(os.sep, "/")
                self.resource_images_path_dict[img_filename] = img_path_rel

    def process_local_images(self, img_loc):
        # From Resources
        if img_loc in self.resource_images_path_dict:
            return self.resource_images_path_dict[img_loc]
        img_filename = sentence_to_snake(img_loc.split("/")[-1]).replace("_", "-")
        file_folder = "/".join(
            self.notebook_path.split("/")[:-1]
        )  # path read from manifest file
        img_input_path = os.path.join(self.input_dir, file_folder, img_loc).replace(
            "/", os.sep
        )
        if not os.path.exists(img_input_path):
            raise ValueError(
                f"Could not get image from {img_loc} from {img_input_path}"
            )
        img_path = os.path.join(self.output_img_dir, img_filename)
        img_path_rel = os.path.relpath(
            img_path, os.path.dirname(self.output_file)
        ).replace(os.sep, "/")
        shutil.copy(img_input_path, img_path)
        return img_path_rel

    def process_external_images(self, img_loc, output_img_dir):
        if self._is_valid_url(img_loc):
            # downloaded image
            response = requests.get(img_loc)
            if response.status_code == 200:
                img_filename = sentence_to_snake(img_loc.split("/")[-1])
                if not os.path.exists(output_img_dir):
                    os.makedirs(output_img_dir)
                img_path = os.path.join(output_img_dir, img_filename)
                with open(img_path, "wb") as img_file:
                    img_file.write(response.content)
                img_path_rel = os.path.relpath(
                    img_path, os.path.dirname(self.output_file)
                ).replace(os.sep, "/")
                return img_path_rel
            else:
                raise ValueError(f"Could not download image from {img_loc}")

    def _is_valid_url(self, url):
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False

    def _replace_img_tag(self, img_tag, img_path_rel):
        img_name = img_path_rel.split("/")[-1].split(".")[0].replace("-", " ")
        img_tag.replace_with(
            f':::image type="content" source="{img_path_rel}" '
            f'alt-text="{img_tag.get("alt", img_name)}":::'
        )

    def run(self):
        self.parse_html()
        self.manage_images()


def compare_doc(fabric_file_path, generated):
    if fabric_file_path:
        with open(fabric_file_path, "r") as f:
            md_content = f.readlines()
    differ = difflib.Differ()
    diff = differ.compare(md_content, generated.splitlines())
    diff_with_row_numbers = [
        (line[0], line[2:])
        for line in diff
        if line.startswith("+") or line.startswith("-")
    ]
    diff_with_row_numbers = [
        (line[0], line[1], index + 1)
        for index, line in enumerate(diff_with_row_numbers)
    ]
    return "\n".join(
        f"{symbol} {line} (row {row_num})"
        for symbol, line, row_num in diff_with_row_numbers
    )


def sentence_to_snake(path: str):
    return (
        path.lower()
        .replace(" - ", "-")
        .replace("_", "-")
        .replace(" ", "-")
        .replace(",", "")
        .replace(".ipynb", "")
        .replace(".rst", "")
    )
