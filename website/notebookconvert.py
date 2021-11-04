import os
import re

def add_header_to_markdown(folder, md):
    name = md[:-3]
    with open(os.path.join(folder, md), 'r+', encoding='utf-8') as f:
        content = f.read()
        f.truncate(0)
        content = re.sub(r'style=\"[\S ]*?\"', '', content)
        content = re.sub(r'<style[\S \n.]*?</style>', '', content)
        f.seek(0, 0)
        f.write("---\ntitle: {}\nhide_title: true\nstatus: stable\n---\n".format(name) + content)
        f.close()

def convert_notebook_to_markdown(folder, nb, outputdir):
    file_path = os.path.join(folder, nb)
    print(f"Converting {file_path} into markdown \n")

    # If the notebook contains cell outputs such as figures, a folder containing cell output images is generated alongside the markdown file
    # By default, both the folder and files contain the notebook name. But a notebook name with spaces results in linking errors
    # Therefore, we first generate the markdown file, output folder, and output files with no spaces
    nb_no_spaces = nb.replace(" ", "").replace(".ipynb", "")

    convert_cmd = f'jupyter nbconvert --output-dir="{outputdir}" --NbConvertApp.output_base="{nb_no_spaces}" --to markdown "{file_path}"'
    os.system(convert_cmd)

    # Afterwards, we rename the generated markdown file to ensure that the markdown file has the same name as notebook
    md_no_spaces = os.path.join(outputdir, f"{nb_no_spaces}.md")
    md_final = os.path.join(outputdir, nb.replace(".ipynb", ".md"))
    os.rename(md_no_spaces,  md_final)

def convert_allnotebooks_in_folder(folder, outputdir):

    dic = {
        "CognitiveServices - Overview": os.path.join(outputdir, "features"),
        "Classification": os.path.join(outputdir, "examples", "classification"),
        "CognitiveServices": os.path.join(outputdir, "examples", "cognitive_services"),
        "DataBalanceAnalysis": os.path.join(outputdir, "examples", "responsible_ai"),
        "DeepLearning": os.path.join(outputdir, "examples", "deep_learning"),
        "Interpretability": os.path.join(outputdir, "examples", "responsible_ai"),
        "Regression": os.path.join(outputdir, "examples", "regression"),
        "TextAnalytics": os.path.join(outputdir, "examples", "text_analytics"),
        "HttpOnSpark": os.path.join(outputdir, "features", "http"),
        "LightGBM": os.path.join(outputdir, "features", "lightgbm"),
        "ModelInterpretability": os.path.join(outputdir, "features", "responsible_ai"),
        "ONNX": os.path.join(outputdir, "features", "onnx"),
        "SparkServing": os.path.join(outputdir, "features", "spark_serving"),
        "Vowpal Wabbit": os.path.join(outputdir, "features", "vw")
        }

    for nb in os.listdir(folder):
        if nb.endswith(".ipynb"):

            finaldir = os.path.join(outputdir, "examples")

            for k, v in dic.items():
                if nb.startswith(k):
                    finaldir = v
                    break
            
            if not os.path.exists(finaldir):
                os.mkdir(finaldir)

            md = nb.replace(".ipynb", ".md")
            if os.path.exists(os.path.join(finaldir, md)):
                os.remove(os.path.join(finaldir, md))

            convert_notebook_to_markdown(folder, nb, finaldir)
            add_header_to_markdown(finaldir, md)

def main():
    cur_path = os.getcwd()
    folder = os.path.join(cur_path, "notebooks")
    outputdir = os.path.join(cur_path, "website", "docs")
    convert_allnotebooks_in_folder(folder, outputdir)

if __name__ == '__main__':
    main()
