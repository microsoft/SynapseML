import io
import os
import re


def add_header_to_markdown(folder, md):
    name = md[:-3]
    with io.open(os.path.join(folder, md), "r+", encoding="utf-8") as f:
        content = f.read()
        f.truncate(0)
        content = re.sub(r"style=\"[\S ]*?\"", "", content)
        content = re.sub(r"<style[\S \n.]*?</style>", "", content)
        f.seek(0, 0)
        f.write(
            "---\ntitle: {}\nhide_title: true\nstatus: stable\n---\n".format(name)
            + content
        )
        f.close()


def convert_notebook_to_markdown(file_path, outputdir):
    print("Converting {} into markdown".format(file_path))
    convert_cmd = 'jupyter nbconvert --output-dir="{}" --to markdown "{}"'.format(
        outputdir, file_path
    )
    os.system(convert_cmd)
    print()


def convert_allnotebooks_in_folder(folder, outputdir):

    cur_folders = [folder]
    output_dirs = [outputdir]
    while cur_folders:
        cur_dir = cur_folders.pop(0)
        cur_output_dir = output_dirs.pop(0)
        for file in os.listdir(cur_dir):
            if os.path.isdir(os.path.join(cur_dir, file)):
                cur_folders.append(os.path.join(cur_dir, file))
                output_dirs.append(os.path.join(cur_output_dir, file))
            else:
                if not os.path.exists(cur_output_dir):
                    os.mkdir(cur_output_dir)

                md = file.replace(".ipynb", ".md")
                if os.path.exists(os.path.join(cur_output_dir, md)):
                    os.remove(os.path.join(cur_output_dir, md))

                convert_notebook_to_markdown(
                    os.path.join(cur_dir, file), cur_output_dir
                )
                add_header_to_markdown(cur_output_dir, md)


def main():
    cur_path = os.getcwd()
    folder = os.path.join(cur_path, "notebooks")
    outputdir = os.path.join(cur_path, "website", "docs")
    convert_allnotebooks_in_folder(folder, outputdir)


if __name__ == "__main__":
    main()
