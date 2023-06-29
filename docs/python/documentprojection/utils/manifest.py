from .logging import *

import yaml

log = get_log(__name__)


def parse_manifest(manifest_path):
    PARSER_VERSION = 0.1
    with open(manifest_path, "r") as file:
        try:
            data = yaml.safe_load(file)
            if float(data["version"]) > PARSER_VERSION:
                raise Exception(
                    f"Manifest version {data['version']} is greater than parser version {PARSER_VERSION}. Failing."
                )
            return data
        except yaml.YAMLError as error:
            log.error("Failed to parse manifest file. Failing.")
            raise error
