
import difflib
from nbconvert.preprocessors import Preprocessor

class LearnDocPreprocessor(Preprocessor):
    def __init__(self, remove_tags=None, **kwargs):
        """
        Initializes the preprocessor with optional remove tags.
        :param remove_tags: A list of tags based on which cells will be removed.
        """
        super(LearnDocPreprocessor, self).__init__(**kwargs)
        self.remove_tags = remove_tags if remove_tags is not None else []
        
    def preprocess(self, nb, resources):
        """
        Preprocess the entire notebook, removing cells tagged with 'remove'
        and adding '> ' to Markdown cells tagged with 'alert' or 'important'.
        """
        if self.remove_tags:
            nb.cells = [
                cell for cell in nb.cells
                if not set(self.remove_tags).intersection(cell.metadata.get('tags', []))
            ]
        
        for index, cell in enumerate(nb.cells):
            nb.cells[index], resources = self.process_cell(cell, resources, index)
        return nb, resources
    
    def add_auto_prereqs(self):
        prerequisites = ["## Prerequisites\n\n[!INCLUDE [prerequisites](includes/prerequisites.md)]"]
        prerequisites.append("- Attach your notebook to a lakehouse. On the left side, select **Add** to add an existing lakehouse or create a lakehouse.")
        return "\n".join(prerequisites)

    def process_cell(self, cell, resources, index):
        """
        Adds '> ' before Markdown cells tagged with 'alert' and 'important'.
        """
        if cell.cell_type == 'markdown' and ('tags' in cell.metadata) and ('alert' in cell.metadata['tags']):
            for tag in cell.metadata['tags']:
                if tag in ['note', 'tip', 'important', 'warning', 'caution']:
                    head = f"> [!{tag.upper()}]\n"
                    cell.source = head + '\n'.join('> ' + line for line in cell.source.splitlines() if not line.startswith(f"## {tag.capitalize()}"))
        if index == 1 and cell.cell_type == 'markdown':
            cell.source = self.add_auto_prereqs() + '\n' + cell.source
        return cell, resources
    
def compare_doc(fabric_file_path, generated):
    if fabric_file_path:
        with open(fabric_file_path, "r") as f:
            md_content = f.readlines()
    differ = difflib.Differ()
    diff = differ.compare(md_content, generated.splitlines())
    diff_with_row_numbers = [(line[0], line[2:]) for line in diff if line.startswith("+") or line.startswith("-")]
    diff_with_row_numbers = [(line[0], line[1], index + 1) for index, line in enumerate(diff_with_row_numbers)]
    return "\n".join(f"{symbol} {line} (row {row_num})" for symbol, line, row_num in diff_with_row_numbers)