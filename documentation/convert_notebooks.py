import os
from datetime import datetime
import yaml
import warnings
from nbconvert import MarkdownExporter
from yaml.loader import FullLoader



def convert_notebook_to_md(input_file):
    """
    convert notebook (.ipynb) file to markdown format

    :param input_file: jupyter notebook
    :type input_file: str
    :return: converted md file
    :rtype: str
    """
    md_exporter = MarkdownExporter()
    nb_body, _ = md_exporter.from_filename(input_file)
    return nb_body

def change_doc_link_absolute():
    pass

def handle_multiple_h1s():
    """
    Azure doc validation rule (Warning) multiple-h1s
    """
    pass


class Document:
    def __init__(self, filename, content):
        self.filename = filename
        self.content = content
        self.input_path = content["input_path"]
        self.output_dir = content["output_dir"]

    def azure_doc_requirement_check(self):
        # TODO: adding checks such as title can only contains lower case letter and "-"
        pass

    def generate_metadata(self):
        """
        take a file and the authors name, generate metadata
        metadata requirements: https://learn.microsoft.com/en-us/contribute/metadata

        Can not use notebook authors as Azure Doc authors.
        Azure Doc require MS authors and contributors need to make content contributions through the private repository
        so the content can be staged and validated by the current validation rules. (Jan 4th, 2023)
        """
        metadata = self.content["metadata"]
        # ensure required metadata info
        for required_metadata in {"author", "description", "ms.author", "ms.topic","title"}:
            if required_metadata not in metadata:
                raise ValueError("{required_metadata} is required metadata by azure doc, please add it to yml file".format(required_metadata=required_metadata))
        if ('ms.service' not in metadata) and ('ms.prod' not in metadata):
            raise ValueError("either ms.service or ms.prod must be in metadata, please add it to yml file")
        # generate final metadata
        generated_metadata = "---\n"
        for k, v in metadata.items():
            generated_metadata += "{k}: {v}\n".format(k=k, v=v)
        if 'ms.date' not in metadata:
            update_date = datetime.today().strftime("%m/%d/%Y")
            generated_metadata += "ms.date: {update_date}\n".format(update_date=update_date)
        else:
            warnings.warn("ms.date is set in yml file, the date won't be automatically updated. if you want the date to be updated automatically, remove ms.date from yml file")

        generated_metadata += "---\n"
        return generated_metadata
    
    def combine_documentation(self, generated_metadata, body):
        """
        combine documentation with metadata, and platform specified info
        """
        if "front" in content:
            with open(content["front"], 'r') as f:
                front = f.read()
        else:
            front = ""
        
        if "end" in content:
            with open(content["end"], 'r') as f:
                end = f.read()
        else:
            end = ""

        generated_doc ="".join([generated_metadata, front, body, end])
        return generated_doc



    def run(self):
        body = convert_notebook_to_md(self.input_path)
        generated_metadata = self.generate_metadata()
        combined_documentation = self.combine_documentation(generated_metadata, body)

        if not os.path.exists(self.output_dir):
           os.mkdir(self.output_dir)
        output_file = self.output_dir + "/" +self.filename + '.md'
        with open(output_file,'w') as f:
            f.write(combined_documentation)

if __name__ == "__main__":
    with open("azure_doc_structure.yml","r") as f:
        structure = yaml.load(f, Loader=FullLoader)
    for doc_name, content in structure.items():
        if content["active"]: # TODO: adding try except, default to active
            doc = Document(doc_name, content)
            doc.run()