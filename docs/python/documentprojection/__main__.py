from __future__ import absolute_import

from .utils.manifest import parse_manifest
from .utils.reflection import *
from .utils.logging import *
from .utils.notebook import *
from .framework.pipeline import *
from .channels import default_channels

import re

log = get_log(__name__)


def get_channel_map(custom_channels_folder, cwd):

    sys.path.insert(0, "documentprojection")

    def camel_to_snake(name):
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        s2 = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
        return s2.replace("_channel", "")

    channels = default_channels.copy()
    if custom_channels_folder is not None and len(custom_channels_folder) > 0:
        channels.extend(get_channels_from_dir(custom_channels_folder, cwd))
    log.info(f"All channels: {channels}")

    channel_map = {
        k: v
        for k, v in [
            (camel_to_snake(channel.__name__), channel) for channel in channels
        ]
    }
    return channel_map


def parse_args():
    log_level_choices = ["debug", "info", "warn", "error", "critical"]

    import argparse

    parser = argparse.ArgumentParser(description="Document Projection Pipeline")
    parser.add_argument(
        "project_root",
        metavar="ROOT",
        type=str,
        help="the root directory of the project",
        default=".",
    )
    parser.add_argument(
        "manifest",
        metavar="MANIFEST",
        type=str,
        help="a notebook or folder of notebooks to project",
        default="docs/manifest.yaml",
    )
    parser.add_argument(
        "-f", "--format", action="store_true", default=False, help="run only formatters"
    )
    parser.add_argument(
        "-p",
        "--publish",
        action="store_true",
        default=False,
        help="run publishers. forces -t and -f.",
    )
    parser.add_argument(
        "-c",
        "--channels",
        default="console",
        type=str,
        help="A channel or comma-separated list of channels through which the notebook(s) should be processed. defaults to console if not specified.",
    )
    parser.add_argument(
        "--customchannels",
        type=str,
        default=None,
        help="A folder containing custom channel implementations.",
    )
    parser.add_argument(
        "-v",
        "--loglevel",
        choices=log_level_choices,
        default="info",
        help="set log level",
    )
    return parser.parse_args()


def run():
    args = parse_args()
    config_log(args.loglevel)
    log.debug("script executed with args: {}".format(args))

    args.project_root = os.path.abspath(args.project_root)

    if args.manifest is not None:
        import json

        log.info(f"Reading manifest file: {args.manifest}.")
        args.manifest = parse_manifest(args.manifest)
        log.debug(f"Manifest:\n{json.dumps(args.manifest, indent=4, sort_keys=True)}")

    channel_map = get_channel_map(args.customchannels, args.project_root)
    pipeline = DocumentProjectionPipeline(
        channel_map, config=PipelineConfig(vars(args))
    )
    pipeline.run()


run()
