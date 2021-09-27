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
    for nb in os.listdir(folder):
        if nb.endswith(".ipynb"):

            finaldir = os.path.join(outputdir, "examples")

            if nb.startswith("CognitiveServices - Overview") or nb.startswith("ONNX"):
                finaldir = os.path.join(outputdir, "features")
            elif nb.startswith("Classification"):
                finaldir = os.path.join(outputdir, "examples", "classification")
            elif nb.startswith("CognitiveServices"):
                finaldir = os.path.join(outputdir, "examples", "cognitive_services")
            elif nb.startswith("DeepLearning"):
                finaldir = os.path.join(outputdir, "examples", "deep_learning")
            elif nb.startswith("Interpretability"):
                finaldir = os.path.join(outputdir, "examples", "model_interpretability")
            elif nb.startswith("Regression"):
                finaldir = os.path.join(outputdir, "examples", "regression")
            elif nb.startswith("TextAnalytics"):
                finaldir = os.path.join(outputdir, "examples", "text_analytics")
            elif nb.startswith("HttpOnSpark"):
                finaldir = os.path.join(outputdir, "features", "http")
            elif nb.startswith("LightGBM"):
                finaldir = os.path.join(outputdir, "features", "lightgbm")
            elif nb.startswith("ModelInterpretability"):
                finaldir = os.path.join(outputdir, "features", "model_interpretability")
            elif nb.startswith("SparkServing"):
                finaldir = os.path.join(outputdir, "features", "spark_serving")
            elif nb.startswith("Vowpal Wabbit"):
                finaldir = os.path.join(outputdir, "features", "vw")
            
            if not os.path.exists(finaldir):
                os.mkdir(finaldir)
            
            convert_notebook_to_markdown(os.path.join(folder, nb), finaldir)
            md = nb.replace(".ipynb", ".md")
            add_header_to_markdown(finaldir, md)

def main():
    cur_path = os.getcwd()
    folder = os.path.join(cur_path, "notebooks")
    outputdir = os.path.join(cur_path, "website", "docs")
    convert_allnotebooks_in_folder(folder, outputdir)

if __name__ == '__main__':
    main()
