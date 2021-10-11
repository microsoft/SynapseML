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

def convert_notebook_to_markdown(file_path, outputdir):
    print("Converting {} into markdown \n".format(file_path))
    convert_cmd = 'jupyter nbconvert --output-dir=\"{}\" --to markdown \"{}\"'.format(outputdir, file_path)
    os.system(convert_cmd)

def convert_allnotebooks_in_folder(folder, outputdir):

    dic = {
        "CognitiveServices - Overview": os.path.join(outputdir, "features"),
        "Classification": os.path.join(outputdir, "examples", "classification"),
        "CognitiveServices": os.path.join(outputdir, "examples", "cognitive_services"),
        "DeepLearning": os.path.join(outputdir, "examples", "deep_learning"),
        "Interpretability": os.path.join(outputdir, "examples", "model_interpretability"),
        "Regression": os.path.join(outputdir, "examples", "regression"),
        "TextAnalytics": os.path.join(outputdir, "examples", "text_analytics"),
        "HttpOnSpark": os.path.join(outputdir, "features", "http"),
        "LightGBM": os.path.join(outputdir, "features", "lightgbm"),
        "ModelInterpretability": os.path.join(outputdir, "features", "model_interpretability"),
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

            convert_notebook_to_markdown(os.path.join(folder, nb), finaldir)
            add_header_to_markdown(finaldir, md)

def main():
    cur_path = os.getcwd()
    folder = os.path.join(cur_path, "notebooks")
    outputdir = os.path.join(cur_path, "website", "docs")
    convert_allnotebooks_in_folder(folder, outputdir)

if __name__ == '__main__':
    main()
