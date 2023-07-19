from channels import WebsiteChannel
import argparse
import yaml
from core import DocumentProcessor
import importlib


def instantiate_channel(channel_yml):
    name = channel_yml["name"]
    if name == "website":
        return WebsiteChannel(channel_yml["input_dir"], channel_yml["output_dir"])
    else:
        module_name, class_name = name.rsplit(".", 1)
        print(
            f"Could not find channel in defaults, attempting to hotload {class_name} from module {module_name}"
        )
        clazz = getattr(importlib.import_module(module_name), class_name)
        kwargs = vars(channel_yml)
        kwargs.remove("name")
        return clazz(**kwargs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Document Projection Pipeline")
    parser.add_argument(
        "--manifest",
        type=str,
        help="A manifest file with the configuration of the pipeline",
        default="manifest.yaml",
    )
    args = parser.parse_args()
    print("Executing with args: {}".format(args))

    with open(args.manifest, "r") as f:
        parsed_manifest = yaml.safe_load(f)
        print("Found Manifest:")
        print(parsed_manifest)

    channels = [instantiate_channel(c) for c in parsed_manifest["channels"]]
    DocumentProcessor(channels).run()
